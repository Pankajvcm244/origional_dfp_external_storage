"""
DFP External Storage - S3-Compatible Cloud Storage for Frappe/ERPNext

This module provides seamless integration between Frappe's File doctype and 
S3-compatible storage services (AWS S3, Minio, etc.).

Architecture:
- DFPExternalStorage: Configuration for S3 bucket connections
- MinioConnection: Wrapper around Minio Python client
- DFPExternalStorageFile: Enhanced File doctype with S3 capabilities
- S3FileProxy: Memory-efficient file streaming handler
- DFPExternalStorageFileRenderer: Custom URL routing for S3 files

Key Features:
- Memory-efficient streaming (no full file loading)
- Smart caching for small public files
- Reference counting for safe deletion
- File deduplication (same content_hash reuses S3 object)
- Presigned URLs for direct S3 access
- Folder-based storage assignment
"""

import os
import re
import io
import mimetypes
import typing as t
from datetime import timedelta
from urllib.parse import urlparse
from werkzeug.wrappers import Response
from werkzeug.wsgi import wrap_file
from functools import cached_property
from minio import Minio
import frappe
from frappe import _
from frappe.core.doctype.file.file import File
from frappe.core.doctype.file.file import URL_PREFIXES
from frappe.model.document import Document
from frappe.utils.password import get_decrypted_password


# ============================================================================
# CONSTANTS
# ============================================================================

# Cache prefix for public files stored in Redis
DFP_EXTERNAL_STORAGE_PUBLIC_CACHE_PREFIX = "external_storage_public_file:"

# URL segment for file access: /file/{file_id}/{filename}
# Example: http://myhost.localhost:8000/file/c7baa5b2ff/my-image.png
DFP_EXTERNAL_STORAGE_URL_SEGMENT_FOR_FILE_LOAD = "file"

# Performance Constants
MIN_STREAM_BUFFER_SIZE = 8192  # 8KB - minimum for efficient streaming
DEFAULT_CACHE_SIZE_LIMIT = 5 * 1024 * 1024  # 5MB - cache files smaller than this
DEFAULT_CACHE_EXPIRATION = 60 * 60 * 24  # 1 day - cache expiration time
DEFAULT_PRESIGNED_URL_EXPIRATION = 60 * 60 * 3  # 3 hours - presigned URL expiration

# S3 Key Pattern
# With doctype: {site}/{year}/{DocType}/{DocName} {FileName}-{Random6}.{ext}
# Without doctype: {site}/{year}/Unspecified/{FileName}-{Random6}.{ext}
RANDOM_SUFFIX_LENGTH = 6  # Length of random suffix in S3 keys
MAX_S3_KEY_LENGTH = 1024  # Maximum S3 key length in bytes

# Fields that require S3 connection validation
DFP_EXTERNAL_STORAGE_CONNECTION_FIELDS = [
    "type",
    "endpoint",
    "secure",
    "bucket_name",
    "region",
    "access_key",
    "secret_key",
]

# Critical fields that affect existing files
DFP_EXTERNAL_STORAGE_CRITICAL_FIELDS = [
    "type",
    "endpoint",
    "secure",
    "bucket_name",
    "region",
    "access_key",
    "secret_key",
    "folders",
]


class S3FileProxy:
    """
    File-like proxy object for streaming S3 files without loading into memory.
    
    Implements the file interface (read, seek, tell) to enable memory-efficient
    streaming of large files from S3. Used with libraries that expect file-like
    objects (e.g., zipfile, PIL).
    
    Attributes:
        readFn: Callable that reads data from S3 given offset and size
        object_size: Total size of the S3 object in bytes
        offset: Current read position in the file
        
    Example:
        with file_doc.dfp_external_storage_file_proxy() as proxy:
            # Use proxy like a regular file
            data = proxy.read(1024)
            proxy.seek(0)
    """

    def __init__(self, readFn, object_size):
        """
        Initialize S3 file proxy.
        
        Args:
            readFn: Function that reads data from S3 (offset, size) -> bytes
            object_size: Total size of the S3 object in bytes
        """
        self.readFn = readFn
        self.object_size = object_size
        self.offset = 0

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit"""
        pass

    def seek(self, offset, whence=0):
        """
        Change stream position.
        
        Args:
            offset: Position offset
            whence: Reference point (SEEK_SET, SEEK_CUR, SEEK_END)
        """
        if whence == io.SEEK_SET:
            self.offset = offset
        elif whence == io.SEEK_CUR:
            self.offset = self.offset + offset
        elif whence == io.SEEK_END:
            self.offset = self.object_size + offset

    def seekable(self):
        """Return whether object supports random access"""
        return True

    def tell(self):
        """Return current stream position"""
        return self.offset

    def read(self, size=0):
        """
        Read and return up to size bytes from S3.
        
        Args:
            size: Number of bytes to read (0 = read all remaining)
            
        Returns:
            bytes: Data read from S3
        """
        content = self.readFn(self.offset, size)
        self.offset = self.offset + len(content)
        return content


class DFPExternalStorage(Document):
    """
    DFP External Storage DocType - Configuration for S3-compatible storage.
    
    Manages connections to S3-compatible storage services and provides
    configuration for file upload/download operations.
    
    Key Fields:
        endpoint: S3 endpoint URL
        access_key: S3 access key
        secret_key: S3 secret key (encrypted)
        bucket_name: S3 bucket name
        region: S3 region
        folders: Child table of folders using this storage
        enabled: Enable/disable this storage
        
    Performance Settings:
        stream_buffer_size: Buffer size for streaming (min 8KB)
        cache_files_smaller_than: Cache threshold in bytes
        cache_expiration_secs: Cache expiration time
        presigned_url_enabled: Enable presigned URLs
        presigned_url_expiration: Presigned URL expiration time
    """

    def validate(self):
        """
        Validate storage configuration before save.
        
        Checks:
        - Stream buffer size meets minimum requirement
        - Connection parameters changed (requires revalidation)
        - Critical fields changed (warns if files exist)
        """
        def has_changed(doc_a: Document, doc_b: Document, fields: list):
            """Check if any of the specified fields have changed"""
            for param in fields:
                value_a = getattr(doc_a, param)
                value_b = getattr(doc_b, param)
                if type(value_a) == list:
                    if not [i.name for i in value_a] == [i.name for i in value_b]:
                        return True
                elif value_a != value_b:
                    return True
            return False

        # Enforce minimum stream buffer size for efficient streaming
        if self.stream_buffer_size < MIN_STREAM_BUFFER_SIZE:
            frappe.msgprint(
                _("Stream buffer size must be at least {0} bytes (8KB).").format(MIN_STREAM_BUFFER_SIZE)
            )
            self.stream_buffer_size = MIN_STREAM_BUFFER_SIZE

        # Recheck S3 connection if needed
        previous = self.get_doc_before_save()
        if previous:
            if self.files_within and has_changed(
                self, previous, DFP_EXTERNAL_STORAGE_CRITICAL_FIELDS
            ):
                frappe.msgprint(
                    _(
                        "There are {} files using this bucket. The field you just updated is critical, be careful!"
                    ).format(self.files_within)
                )
        if not previous or has_changed(
            self, previous, DFP_EXTERNAL_STORAGE_CONNECTION_FIELDS
        ):
            self.validate_bucket()

    def on_trash(self):
        if self.files_within:
            frappe.throw(
                _("Can not be deleted. There are {} files using this bucket.").format(
                    self.files_within
                )
            )

    @cached_property
    def setting_stream_buffer_size(self):
        """
        Get effective stream buffer size with minimum enforcement.
        
        Returns:
            int: Buffer size in bytes (minimum 8KB)
        """
        return self.stream_buffer_size if self.stream_buffer_size >= MIN_STREAM_BUFFER_SIZE else MIN_STREAM_BUFFER_SIZE

    @cached_property
    def setting_cache_files_smaller_than(self):
        """
        Get cache size threshold.
        
        Files smaller than this size will be cached in Redis for faster access.
        
        Returns:
            int: Size threshold in bytes (default: 5MB)
        """
        return (
            self.cache_files_smaller_than
            if self.cache_files_smaller_than >= 0
            else DEFAULT_CACHE_SIZE_LIMIT
        )

    @cached_property
    def setting_cache_expiration_secs(self):
        """
        Get cache expiration time.
        
        Returns:
            int: Expiration time in seconds (default: 1 day)
        """
        return (
            self.cache_expiration_secs
            if self.cache_expiration_secs >= 0
            else DEFAULT_CACHE_EXPIRATION
        )

    @cached_property
    def setting_presigned_url_expiration(self):
        """
        Get presigned URL expiration time.
        
        Returns:
            int: Expiration time in seconds (default: 3 hours)
        """
        return (
            self.presigned_url_expiration
            if self.presigned_url_expiration > 0
            else DEFAULT_PRESIGNED_URL_EXPIRATION
        )

    @cached_property
    def files_within(self):
        return frappe.db.count("File", filters={"dfp_external_storage": self.name})

    def validate_bucket(self):
        if self.client:
            self.client.validate_bucket(self.bucket_name)

    @cached_property
    def client(self):
        if self.endpoint and self.access_key and self.secret_key and self.region:
            try:
                if self.is_new() and self.secret_key:
                    key_secret = self.secret_key
                else:
                    key_secret = get_decrypted_password(
                        "DFP External Storage", self.name, "secret_key"
                    )
                if key_secret:
                    return MinioConnection(
                        endpoint=self.endpoint,
                        access_key=self.access_key,
                        secret_key=key_secret,
                        region=self.region,
                        secure=self.secure,
                    )
            except:
                pass

    def remote_files_list(self):
        return self.client.list_objects(self.bucket_name, recursive=True)


class MinioConnection:
    def __init__(
        self, endpoint: str, access_key: str, secret_key: str, region: str, secure: bool
    ):
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            region=region,
            secure=secure,
        )

    def validate_bucket(self, bucket_name: str):
        try:
            if self.client.bucket_exists(bucket_name):
                frappe.msgprint(
                    _("Great! Bucket is accesible ;)"), indicator="green", alert=True
                )
                return True
            else:
                frappe.throw(_("Bucket not found"))
        except Exception as e:
            if hasattr(e, "message"):
                frappe.throw(_("Error when looking for bucket: {}".format(e.message)))
            elif hasattr(e, "reason"):
                frappe.throw(str(e))
        return False

    def remove_object(self, bucket_name: str, object_name: str):
        """
        Minio params:
        :param bucket_name: Name of the bucket.
        :param object_name: Object name in the bucket.
        :param version_id: Version ID of the object.
        """
        return self.client.remove_object(
            bucket_name=bucket_name, object_name=object_name
        )

    def stat_object(self, bucket_name: str, object_name: str):
        """
        Minio params:
        :param bucket_name: Name of the bucket.
        :param object_name: Object name in the bucket.
        :param version_id: Version ID of the object.
        """
        return self.client.stat_object(bucket_name=bucket_name, object_name=object_name)

    def get_object(
        self, bucket_name: str, object_name: str, offset: int = 0, length: int = 0
    ):
        """
        Minio params:
        :param bucket_name: Name of the bucket.
        :param object_name: Object name in the bucket.
        :param offset: Start byte position of object data.
        :param length: Number of bytes of object data from offset.
        :param request_headers: Any additional headers to be added with GET request.
        :param ssec: Server-side encryption customer key.
        :param version_id: Version-ID of the object.
        :param extra_query_params: Extra query parameters for advanced usage.
        :return: :class:`urllib3.response.HTTPResponse` object.
        """
        return self.client.get_object(
            bucket_name=bucket_name,
            object_name=object_name,
            offset=offset,
            length=length,
        )

    def fget_object(self, bucket_name: str, object_name: str, file_path: str):
        """
        Minio params:
        :param bucket_name: Name of the bucket.
        :param object_name: Object name in the bucket.
        :param file_path: Name of file to download
        :param request_headers: Any additional headers to be added with GET request.
        :param ssec: Server-side encryption customer key.
        :param version_id: Version-ID of the object.
        :param extra_query_params: Extra query parameters for advanced usage.
        :param temp_file_path: Path to a temporary file
        :return: :class:`urllib3.response.HTTPResponse` object.
        """
        return self.client.fget_object(
            bucket_name=bucket_name, object_name=object_name, file_path=file_path
        )

    def presigned_get_object(
        self, bucket_name: str, object_name: str, expires: int = timedelta(hours=3)
    ):
        """
        Minio params:
        Get presigned URL of an object to download its data with expiry time
        and custom request parameters.

        :param bucket_name: Name of the bucket.
        :param object_name: Object name in the bucket.
        :param expires: Expiry in seconds; defaults to 7 days.
        :param response_headers: Optional response_headers argument to
                                                                                                        specify response fields like date, size,
                                                                                                        type of file, data about server, etc.
        :param request_date: Optional request_date argument to
                                                                                        specify a different request date. Default is
                                                                                        current date.
        :param version_id: Version ID of the object.
        :param extra_query_params: Extra query parameters for advanced usage.
        :return: URL string.

        Example::
                        # Get presigned URL string to download 'my-object' in
                        # 'my-bucket' with default expiry (i.e. 7 days).
                        url = client.presigned_get_object("my-bucket", "my-object")
                        print(url)

                        # Get presigned URL string to download 'my-object' in
                        # 'my-bucket' with two hours expiry.
                        url = client.presigned_get_object("my-bucket", "my-object", expires=timedelta(hours=2))
                        print(url)
        """
        if type(expires) == int:
            expires = timedelta(seconds=expires)
        return self.client.presigned_get_object(
            bucket_name=bucket_name, object_name=object_name, expires=expires
        )

    def put_object(self, bucket_name, object_name, data, metadata=None, length=-1):
        """
        Minio params:
        :param bucket_name: Name of the bucket.
        :param object_name: Object name in the bucket.
        :param data: An object having callable read() returning bytes object.
        :param length: Data size; -1 for unknown size and set valid part_size.
        :param content_type: Content type of the object.
        :param metadata: Any additional metadata to be uploaded along
                        with your PUT request.
        :param sse: Server-side encryption.
        :param progress: A progress object;
        :param part_size: Multipart part size.
        :param num_parallel_uploads: Number of parallel uploads.
        :param tags: :class:`Tags` for the object.
        :param retention: :class:`Retention` configuration object.
        :param legal_hold: Flag to set legal hold for the object.
        """
        return self.client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=data,
            metadata=metadata,
            length=length,
        )

    def list_objects(self, bucket_name: str, recursive=True):
        """
        Minio params:
        :param bucket_name: Name of the bucket.
        # :param prefix: Object name starts with prefix.
        # :param recursive: List recursively than directory structure emulation.
        # :param start_after: List objects after this key name.
        # :param include_user_meta: MinIO specific flag to control to include
        # 						user metadata.
        # :param include_version: Flag to control whether include object
        # 						versions.
        # :param use_api_v1: Flag to control to use ListObjectV1 S3 API or not.
        # :param use_url_encoding_type: Flag to control whether URL encoding type
        # 							to be used or not.
        :return: Iterator of :class:`Object <Object>`.
        """
        return self.client.list_objects(bucket_name=bucket_name, recursive=recursive)


class DFPExternalStorageFile(File):
    def __init__(self, *args, **kwargs):
        super(DFPExternalStorageFile, self).__init__(*args, **kwargs)

    def before_insert(self):
        """
        Called before inserting a new File document.
        
        Handles file reuse (amendment/library) by detecting if the file_url
        references an existing S3 file and setting is_remote_file flag.
        
        This runs BEFORE validation, so we can set flags to bypass URL validation.
        This aligns with Frappe's default behavior of copying file_url without custom fields.
        """
        # Check if this is a file reuse case (amendment/library attachment)
        # Frappe copies file_url but not custom fields, so we detect S3 files by checking
        # if another File with this file_url has S3 metadata
        if self.file_url and not self.dfp_external_storage_s3_key:
            # Check if this file_url points to an S3-backed file
            original = frappe.db.get_value(
                "File",
                {"file_url": self.file_url, "dfp_external_storage_s3_key": ["!=", ""]},
                ["name"],
                as_dict=True
            )
            
            if original:
                # File reuse detected: Mark as remote to bypass URL validation
                # Set flag that will be used in is_remote_file property and validate_file_on_disk
                self.flags.is_remote_file = True
                self.flags.is_s3_reference = True  # Additional flag for clarity
                
                frappe.logger().info(
                    f"S3 file reuse: File {self.name} references {original.name} via {self.file_url}"
                )
        
        # Call parent before_insert
        super(DFPExternalStorageFile, self).before_insert()

    @property
    def is_remote_file(self):
        """
        Override is_remote_file property to handle both:
        1. Files with S3 keys (actual S3 files)
        2. Files that reference other S3 files via file_url (reused files)
        """
        # Check if flag was set in before_insert (for reused files)
        if hasattr(self.flags, 'is_remote_file') and self.flags.is_remote_file:
            return True
        
        # Check if this file has S3 metadata (actual S3 file)
        if self.dfp_external_storage_s3_key:
            return True
        
        # Fall back to parent implementation
        return super(DFPExternalStorageFile, self).is_remote_file

    @cached_property
    def dfp_external_storage_doc(self):
        dfp_ext_strg_doc = None
        # 1. Use defined
        if self.dfp_external_storage:
            try:
                dfp_ext_strg_doc = frappe.get_doc(
                    "DFP External Storage", self.dfp_external_storage
                )
            except:
                pass
        if not dfp_ext_strg_doc:
            # 2. Specific folder connection
            dfp_ext_strg_name = frappe.db.get_value(
                "DFP External Storage by Folder",
                fieldname="parent",
                filters={"folder": self.folder},
            )
            # 3. Default connection (Home folder)
            if not dfp_ext_strg_name:
                dfp_ext_strg_name = frappe.db.get_value(
                    "DFP External Storage by Folder",
                    fieldname="parent",
                    filters={"folder": "Home"},
                )
            if dfp_ext_strg_name:
                dfp_ext_strg_doc = frappe.get_doc(
                    "DFP External Storage", dfp_ext_strg_name
                )
        return dfp_ext_strg_doc

    def dfp_is_s3_remote_file(self):
        if self.dfp_external_storage_s3_key and self.dfp_external_storage_doc:
            return True

    def dfp_is_cacheable(self):
        return (
            not self.is_private
            and self.dfp_external_storage_doc.setting_cache_files_smaller_than
            and self.dfp_file_size != 0
            and self.dfp_file_size
            < self.dfp_external_storage_doc.setting_cache_files_smaller_than
        )

    @cached_property
    def dfp_file_size(self) -> int:
        if (
            self.dfp_is_s3_remote_file()
            and self.dfp_external_storage_doc.remote_size_enabled
        ):
            try:
                object_info = self.dfp_external_storage_doc.client.stat_object(
                    bucket_name=self.dfp_external_storage_doc.bucket_name,
                    object_name=self.dfp_external_storage_s3_key,
                )
                return object_info.size
            except:
                frappe.log_error(
                    title=f"Error getting remote file size: {self.dfp_external_storage_s3_key}"
                )
        return self.file_size

    @cached_property
    def dfp_external_storage_client(self):
        if self.dfp_external_storage_doc:
            return self.dfp_external_storage_doc.client

    def dfp_external_storage_ignored_doctypes(self):
        "Do not apply for files attached to specified doctypes"
        if (
            self.attached_to_doctype
            and self.dfp_external_storage_doc
            and self.attached_to_doctype
            in [
                i.doctype_to_ignore
                for i in self.dfp_external_storage_doc.doctypes_ignored
            ]
        ):
            frappe.msgprint(
                _(
                    """This doctype does not allow remote files attached to it. Check "DFP External Storage" advanced settings for more details."""
                )
            )
            return True

    def _generate_s3_key(self):
        """
        Generate S3 key with hierarchical folder structure.
        
        S3 Key Pattern:
            With doctype: {site}/{year}/{DocType}/{DocName} {FileName}-{Random6}.{ext}
            Without doctype: {site}/{year}/Unspecified/{FileName}-{Random6}.{ext}
        
        Example:
            hkmjerp.in/25/Purchase Invoice/HKMJ-PI-2512-00897 2044-dyuzya.pdf
            hkmjerp.in/25/Unspecified/report-a3b5c7.xlsx
        
        Returns:
            str: S3 key path (max 1024 bytes)
            
        Raises:
            ValueError: If generated key exceeds MAX_S3_KEY_LENGTH
        """
        import random
        import string
        
        # Get site name (e.g., "hkmjerp.in")
        site_name = frappe.local.site
        
        # Extract 2-digit year from file creation date
        creation_date = self.creation or frappe.utils.now()
        
        if hasattr(creation_date, 'year'):
            year_2digit = str(creation_date.year)[-2:]
        else:
            # Parse string date
            from frappe.utils import getdate
            try:
                parsed_date = getdate(creation_date)
                year_2digit = str(parsed_date.year)[-2:]
            except Exception:
                # Fallback to current year
                year_2digit = str(frappe.utils.now().year)[-2:]
        
        # Split filename into base and extension
        file_name = self.file_name or "file"
        
        if '.' in file_name:
            name_parts = file_name.rsplit('.', 1)
            file_name_base = name_parts[0]
            file_extension = name_parts[1]
        else:
            file_name_base = file_name
            file_extension = ""
        
        # Generate random suffix for uniqueness
        random_suffix = ''.join(
            random.choices(string.ascii_lowercase + string.digits, k=RANDOM_SUFFIX_LENGTH)
        )
        
        # Build S3 key based on document attachment
        if self.attached_to_doctype and self.attached_to_name:
            # Attached to a document: use doctype as folder
            doctype = self.attached_to_doctype  # Preserve spaces
            docname = self.attached_to_name
            
            # Pattern: {site}/{year}/{DocType}/{DocName} {FileName}-{Random6}.{ext}
            if file_extension:
                s3_key = f"{site_name}/{year_2digit}/{doctype}/{docname} {file_name_base}-{random_suffix}.{file_extension}"
            else:
                s3_key = f"{site_name}/{year_2digit}/{doctype}/{docname} {file_name_base}-{random_suffix}"
        else:
            # Not attached: use "Unspecified" folder
            # Pattern: {site}/{year}/Unspecified/{FileName}-{Random6}.{ext}
            if file_extension:
                s3_key = f"{site_name}/{year_2digit}/Unspecified/{file_name_base}-{random_suffix}.{file_extension}"
            else:
                s3_key = f"{site_name}/{year_2digit}/Unspecified/{file_name_base}-{random_suffix}"
        
        # Validate key length (S3 limit is 1024 bytes UTF-8)
        if len(s3_key.encode('utf-8')) > MAX_S3_KEY_LENGTH:
            frappe.throw(
                _("Generated S3 key exceeds maximum length of {0} bytes: {1}").format(
                    MAX_S3_KEY_LENGTH, s3_key[:100] + "..."
                )
            )
        
        return s3_key

    def dfp_external_storage_upload_file(self, local_file=None):
        """
        Upload file to S3 storage and update File document.
        
        This method:
        1. Generates unique S3 key using folder structure
        2. Uploads file from local filesystem to S3
        3. Updates File document with S3 metadata
        4. Deletes local file after successful upload
        
        Critical Fields Updated:
            dfp_external_storage_s3_key: S3 object key
            dfp_external_storage: Storage connection name
            file_url: New URL format /file/{file_id}/{filename}
        
        Args:
            local_file: Path to local file. If None, constructs path from file_url
            
        Returns:
            bool: True if uploaded, False if skipped
            
        Raises:
            FileNotFoundError: If local file doesn't exist
            S3Error: If S3 upload fails
        """
        # Skip if doctype is in ignore list
        if self.dfp_external_storage_ignored_doctypes():
            self.dfp_external_storage = ""
            return False
            
        # Skip if storage is disabled
        if (
            not self.dfp_external_storage_doc
            or not self.dfp_external_storage_doc.enabled
        ):
            return False
            
        # Skip folders
        if self.is_folder:
            return False
            
        # Skip if already on S3
        if self.dfp_external_storage_s3_key:
            return False
            
        # Skip external URLs
        if is_url(self.file_url):
            return False
            
        # Skip HTTP(S) URLs (not implemented)
        if self.file_url and self.file_url.startswith(URL_PREFIXES):
            raise NotImplementedError(
                "HTTP(S) URLs cannot be saved to external storage."
            )

        original_file_url = self.file_url

        # Define S3 key with folder structure
        key = self._generate_s3_key()

        is_public = "/public" if not self.is_private else ""
        if not local_file:
            local_file = "./" + frappe.local.site + is_public + self.file_url

        try:
            # Validate local file exists
            if not os.path.exists(local_file):
                raise FileNotFoundError(f"Local file not found: {local_file}")
                
            # Upload to S3
            with open(local_file, "rb") as f:
                self.dfp_external_storage_client.put_object(
                    bucket_name=self.dfp_external_storage_doc.bucket_name,
                    object_name=key,
                    data=f,
                    length=os.path.getsize(local_file),
                    # Meta removed because same s3 file can be used within different File docs
                    # metadata={"frappe_file_id": self.name}
                )

            # Update File document with S3 metadata
            self.dfp_external_storage_s3_key = key
            self.dfp_external_storage = self.dfp_external_storage_doc.name
            self.file_url = f"/{DFP_EXTERNAL_STORAGE_URL_SEGMENT_FOR_FILE_LOAD}/{self.name}/{self.file_name}"
            
            # Update parent document field if attached
            if self.attached_to_field:
                frappe.db.set_value(
                    self.attached_to_doctype,
                    self.attached_to_name,
                    self.attached_to_field,
                    self.file_url,
                    update_modified=False
                )
            
            # Delete local file after successful upload
            os.remove(local_file)
            
        except FileNotFoundError as e:
            error_msg = _("Local file not found: {0}").format(self.file_name)
            frappe.log_error(title="S3 Upload - File Not Found", message=str(e))
            
            # For new files, fall back to local storage
            if not self.get_doc_before_save():
                self.dfp_external_storage = ""
                self.dfp_external_storage_s3_key = ""
                self.file_url = original_file_url
            else:
                frappe.throw(error_msg)
                
        except OSError as e:
            error_msg = _("File system error while uploading {0}: {1}").format(
                self.file_name, str(e)
            )
            frappe.log_error(title="S3 Upload - File System Error", message=error_msg)
            
            # For new files, fall back to local storage
            if not self.get_doc_before_save():
                self.dfp_external_storage = ""
                self.dfp_external_storage_s3_key = ""
                self.file_url = original_file_url
            else:
                frappe.throw(error_msg)
                
        except Exception as e:
            # Catch S3 errors and other exceptions
            error_msg = _("Error uploading file {0} to S3: {1}").format(
                self.file_name, str(e)
            )
            frappe.log_error(title="S3 Upload Failed", message=error_msg)
            
            # For new files, fall back to local storage
            if not self.get_doc_before_save():
                frappe.log_error(
                    title="S3 Upload Failed - Falling back to local storage",
                    message=f"File {self.file_name} saved locally instead"
                )
                self.dfp_external_storage_s3_key = ""
                self.dfp_external_storage = ""
                self.file_url = original_file_url
            # If modifying existing file, throw error
            else:
                frappe.throw(error_msg)

    def dfp_external_storage_delete_file(self):
        """
        Delete S3 object with reference counting for safety.
        
        This method implements reference counting to prevent accidental deletion
        of S3 objects that are shared by multiple File documents (e.g., during
        document amendment or library file attachment).
        
        Deletion Logic:
        1. Skip if not an S3 file
        2. Check if other File documents reference the same S3 object
        3. Only delete S3 object if this is the last reference
        4. Always delete the File document itself
        
        This ensures:
        - Shared S3 objects are preserved until all references are deleted
        - No broken file links after document amendment
        - Storage efficiency through file deduplication
        
        Raises:
            PermissionError: If storage connection is disabled
            S3Error: If S3 deletion fails
        """
        # Skip if not an S3 file
        if not self.dfp_is_s3_remote_file():
            return
            
        # Reference counting: check if other File docs use the same S3 object
        files_using_s3_key = frappe.get_all(
            "File",
            filters={
                "dfp_external_storage_s3_key": self.dfp_external_storage_s3_key,
                "dfp_external_storage": self.dfp_external_storage,
            },
        )
        
        # If other File documents reference this S3 object, don't delete it
        if len(files_using_s3_key):
            frappe.log_error(
                title="S3 Object Preserved",
                message=f"S3 object {self.dfp_external_storage_s3_key} has {len(files_using_s3_key)} reference(s). Not deleting."
            )
            return
            
        # Validate storage connection is enabled
        if (
            not self.dfp_external_storage_doc
            or not self.dfp_external_storage_doc.enabled
        ):
            error_msg = _("Cannot delete S3 file: Write disabled for connection <strong>{0}</strong>").format(
                self.dfp_external_storage_doc.title if self.dfp_external_storage_doc else "Unknown"
            )
            frappe.throw(error_msg)
            
        # Delete S3 object
        try:
            self.dfp_external_storage_client.remove_object(
                bucket_name=self.dfp_external_storage_doc.bucket_name,
                object_name=self.dfp_external_storage_s3_key,
            )
            frappe.log_error(
                title="S3 Object Deleted",
                message=f"Successfully deleted S3 object: {self.dfp_external_storage_s3_key}"
            )
        except Exception as e:
            error_msg = _("Error deleting S3 file {0}: {1}").format(
                self.file_name, str(e)
            )
            frappe.log_error(title="S3 Deletion Failed", message=error_msg)
            frappe.throw(error_msg)

    def dfp_external_storage_download_to_file(self, local_file):
        """
        Stream file from S3 directly to local_file. This avoids reading the whole file into memory at any point
        :param local_file: path to a local file to stream content to
        """
        if not self.dfp_is_s3_remote_file():
            # frappe.msgprint(_("S3 key not found: ") + self.file_name,
            # 	indicator="red", title=_("Error processing File"), alert=True)
            return
        try:
            key = self.dfp_external_storage_s3_key

            self.dfp_external_storage_client.fget_object(
                bucket_name=self.dfp_external_storage_doc.bucket_name,
                object_name=key,
                file_path=local_file,
            )
        except Exception as e:
            error_msg = _(
                "Error downloading to file from remote folder. Check Error Log for more information."
            )
            frappe.log_error(title=f"{error_msg}: {self.file_name}", message=e)
            frappe.throw(error_msg)

    def dfp_external_storage_file_proxy(self):
        """
        Get a read-only context manager file-like object that will read requested bytes directly from S3. This allows you to avoid downloading the whole file when only parts or chunks of it will be read from.
        """
        if not self.dfp_is_s3_remote_file():
            return

        def read_chunks(offset=0, size=0):
            with self.dfp_external_storage_client.get_object(
                bucket_name=self.dfp_external_storage_doc.bucket_name,
                object_name=self.dfp_external_storage_s3_key,
                offset=offset,
                length=size,
            ) as response:
                content = response.read()
            return content

        return S3FileProxy(readFn=read_chunks, object_size=self.dfp_file_size)

    def dfp_external_storage_download_file(self) -> bytes:
        content = b""
        if not self.dfp_is_s3_remote_file():
            return content
        try:
            with self.dfp_external_storage_client.get_object(
                bucket_name=self.dfp_external_storage_doc.bucket_name,
                object_name=self.dfp_external_storage_s3_key,
            ) as response:
                content = response.read()
            return content
        except:
            error_msg = _("Error downloading file from remote folder")
            frappe.log_error(title=f"{error_msg}: {self.file_name}")
            frappe.throw(error_msg)
        return content

    def dfp_external_storage_stream_file(self) -> t.Iterable[bytes]:
        return wrap_file(
            environ=frappe.local.request.environ,
            file=self.dfp_external_storage_file_proxy(),
            buffer_size=self.dfp_external_storage_doc.setting_stream_buffer_size,
        )

    def download_to_local_and_remove_remote(self):
        try:
            bucket = self.dfp_external_storage_doc.bucket_name
            key = self.dfp_external_storage_s3_key

            self.dfp_external_storage_s3_key = ""
            self.dfp_external_storage = ""

            with self.dfp_external_storage_client.get_object(
                bucket_name=bucket, object_name=key
            ) as response:
                self._content = response.read()
            self.save_file_on_filesystem()

            self.dfp_external_storage_client.remove_object(
                bucket_name=bucket, object_name=key
            )
        except Exception as e:
            error_msg = _("Error downloading and removing file from remote folder.")
            frappe.log_error(title=f"{error_msg}: {self.file_name}")
            frappe.throw(error_msg)

    def validate_file_on_disk(self):
        """
        Validate file exists on disk.
        
        Skip validation for:
        1. Actual S3 files (have dfp_external_storage_s3_key)
        2. Reused S3 files (reference other S3 files via file_url)
        
        Aligns with Frappe's is_remote_file behavior.
        """
        # Skip validation for S3 files (actual or referenced)
        if self.is_remote_file:
            return True
        
        # For local files, run normal validation
        return super(DFPExternalStorageFile, self).validate_file_on_disk()

    def exists_on_disk(self):
        """
        Check if file exists on local disk.
        
        Returns False for S3 files (actual or referenced) since they're not on disk.
        """
        # S3 files (actual or referenced) don't exist on local disk
        if self.is_remote_file:
            return False
        
        # For local files, check disk
        return super(DFPExternalStorageFile, self).exists_on_disk()

    @frappe.whitelist()
    def optimize_file(self):
        if self.dfp_is_s3_remote_file():
            raise NotImplementedError("Only local image files can be optimized")
        super(DFPExternalStorageFile, self).optimize_file()

    def _remote_file_local_path_get(self):
        return f"/{DFP_EXTERNAL_STORAGE_URL_SEGMENT_FOR_FILE_LOAD}/{self.name}/{self.file_name}"

    def get_content(self) -> bytes:
        """
        Get file content as bytes.
        
        For S3 files: Streams content from S3
        For local files: Uses Frappe's default implementation
        
        Returns:
            bytes: File content
            
        Raises:
            PermissionError: If file is not downloadable
            S3Error: If S3 download fails
        """
        # Note: File reuse is now handled in hook_file_before_save() - no need to check here
        if not self.dfp_is_s3_remote_file():
            return super(DFPExternalStorageFile, self).get_content()
        try:
            if not self.is_downloadable():
                raise Exception("File not available")
            return self.dfp_external_storage_download_file()
        except Exception:
            # If no document, no read permissions, etc. For security reasons do not give any information, so just raise a 404 error
            raise frappe.PageDoesNotExistError()

    @cached_property
    def dfp_mime_type_guess_by_file_name(self):
        content_type, _ = mimetypes.guess_type(self.file_name)
        if content_type:
            return content_type

    def dfp_presigned_url_get(self):
        if (
            not self.dfp_is_s3_remote_file()
            or not self.dfp_external_storage_doc.presigned_urls
        ):
            return
        if (
            self.dfp_external_storage_doc.presigned_mimetypes_starting
            and self.dfp_mime_type_guess_by_file_name
        ):
            # get list exploding by new line, removing empty lines and cleaning starting and ending spaces
            presigned_mimetypes_starting = [
                i.strip()
                for i in self.dfp_external_storage_doc.presigned_mimetypes_starting.split(
                    "\n"
                )
                if i.strip()
            ]
            if not any(
                self.dfp_mime_type_guess_by_file_name.startswith(i)
                for i in presigned_mimetypes_starting
            ):
                return
        return self.dfp_external_storage_client.presigned_get_object(
            bucket_name=self.dfp_external_storage_doc.bucket_name,
            object_name=self.dfp_external_storage_s3_key,
            expires=self.dfp_external_storage_doc.setting_presigned_url_expiration,
        )


def hook_file_before_save(doc, method):
    """
    This method is called before the document is saved to DB (insert or update row)
    Critical fields: dfp_external_storage_s3_key, dfp_external_storage and file_url
    
    Aligns with Frappe's default behavior:
    - For file reuse (amendment/library): Frappe copies file_url, we skip upload
    - For new uploads: Upload to S3 if storage is selected
    """
    previous = doc.get_doc_before_save()

    if not previous:
        # NEW "File": Check if this is a reused S3 file
        # The is_s3_reference flag was set in before_insert if this file references an S3 file
        if hasattr(doc.flags, 'is_s3_reference') and doc.flags.is_s3_reference:
            # File reuse: Skip upload, DFP fields already empty (Frappe doesn't copy custom fields)
            return
        
        # NEW "File": Upload to S3 if storage is selected
        doc.dfp_external_storage_upload_file()
        
        return

    # MODIFY "File"

    # MODIFY "File": Case 1: Existent local file + new storage selected => upload to remote
    if (
        not doc.dfp_external_storage_s3_key
        and doc.dfp_external_storage
        and not previous.dfp_external_storage
    ):
        doc.dfp_external_storage_upload_file()

    # MODIFY "File": Case 2: Existent remote file + no storage selected => download to local + remove from remote
    elif previous.dfp_external_storage and not doc.dfp_external_storage:
        previous.download_to_local_and_remove_remote()
        doc.file_url = previous.file_url  # << contains local file path downloaded
        ####  To change the Local Field of Document
        if doc.attached_to_field:
            frappe.db.set_value(
                doc.attached_to_doctype,
                doc.attached_to_name,
                doc.attached_to_field,
                doc.file_url,
            )
        ####
        doc.dfp_external_storage_s3_key = ""

    # MODIFY "File": Case 3: Existent remote file + new remote selected => stream from old to new remote + delete from old remote
    elif (
        previous.dfp_external_storage
        and doc.dfp_external_storage
        and previous.dfp_external_storage != doc.dfp_external_storage
    ):
        try:
            # MODIFY "File": Case 3.1.: new remote + not allowed doctype => download to local + remove old remote
            # TODO: Maybe we should left in old remote?? and we should check if old remote allows the doctype??
            if doc.dfp_external_storage_ignored_doctypes():
                previous.download_to_local_and_remove_remote()
                doc.file_url = (
                    previous.file_url
                )  # << contains local file path downloaded
                doc.dfp_external_storage_s3_key = ""
                doc.dfp_external_storage = ""
            # MODIFY "File": Case 3.2.: new remote + allowed doctype => stream from old to new remote + delete from old remote
            else:
                # Get file from previous remote in chunks of 10MB (not loading it fully in memory)
                with previous.dfp_external_storage_file_proxy() as response:
                    doc.dfp_external_storage_client.put_object(
                        bucket_name=doc.dfp_external_storage_doc.bucket_name,
                        object_name=doc.dfp_external_storage_s3_key,
                        data=response,
                        length=response.object_size,
                        # Meta removed because same s3 file can be used within different File docs
                        # metadata={"frappe_file_id": self.name}
                    )
                # New s3 key => update "file_url"
                doc.file_url = doc._remote_file_local_path_get()
                # Remove file from previous remote
                previous.dfp_external_storage_client.remove_object(
                    bucket_name=previous.dfp_external_storage_doc.bucket_name,
                    object_name=previous.dfp_external_storage_s3_key,
                )
        except:
            error_msg = _("Error putting file from one remote to another.")
            frappe.log_error(f"{error_msg}: {doc.file_name}")
            frappe.throw(error_msg)

    # Clean cache when updating "File"
    if doc.dfp_external_storage_s3_key:
        cache_key = f"{DFP_EXTERNAL_STORAGE_PUBLIC_CACHE_PREFIX}{doc.name}"
        frappe.cache().delete_value(cache_key)


def hook_file_on_update(doc, method):
    """DEPRECATED! Remove method after 2025.01.01 ("/dfp_external_storage/dfp_external_storage/hooks.py" too)"""
    pass


def hook_file_on_rename(doc, method, old_name, new_name, merge=False):
    """
    Called when a File document is renamed.
    Handles renaming of S3 objects when the File document name changes.
    """
    if not doc.dfp_is_s3_remote_file():
        return

    if merge:
        # For merge operations, we don't need to rename the S3 object
        # The old file will be deleted by the merge process
        return

    try:
        # Generate new S3 key based on new file name
        base, extension = os.path.splitext(doc.file_name)
        new_key = f"{frappe.local.site}/{base}-{new_name}{extension}"

        # Copy object to new key
        with doc.dfp_external_storage_client.get_object(
            bucket_name=doc.dfp_external_storage_doc.bucket_name,
            object_name=doc.dfp_external_storage_s3_key,
        ) as response:
            doc.dfp_external_storage_client.put_object(
                bucket_name=doc.dfp_external_storage_doc.bucket_name,
                object_name=new_key,
                data=response,
                length=response.headers.get('content-length', -1),
            )

        # Update the S3 key in the document
        doc.dfp_external_storage_s3_key = new_key

        # Update file_url to reflect new name
        doc.file_url = doc._remote_file_local_path_get()

        # Delete old object
        doc.dfp_external_storage_client.remove_object(
            bucket_name=doc.dfp_external_storage_doc.bucket_name,
            object_name=doc.dfp_external_storage_s3_key,
        )

        # Clean cache for the new file name
        cache_key = f"{DFP_EXTERNAL_STORAGE_PUBLIC_CACHE_PREFIX}{new_name}"
        frappe.cache().delete_value(cache_key)

        frappe.msgprint(_("S3 file renamed successfully: {0}").format(doc.file_name))

    except Exception as e:
        error_msg = _("Error renaming file in remote storage: {0}").format(str(e))
        frappe.log_error(f"{error_msg}: {doc.file_name}", message=e)
        frappe.throw(error_msg)


def hook_file_before_delete(doc, method):
    """
    Called before a File document is deleted.
    
    Checks if other File documents reference this file's URL.
    If references exist, prevents deletion and shows error.
    
    This ensures:
    - Files used in amendments/library attachments aren't accidentally deleted
    - Users must delete referencing files first
    - No broken file links
    """
    # Only check for S3 files
    if not doc.dfp_is_s3_remote_file():
        return
    
    # Check if any other File documents reference this file's URL
    referencing_files = frappe.get_all(
        "File",
        filters={
            "file_url": doc.file_url,
            "name": ["!=", doc.name]  # Exclude current file
        },
        fields=["name", "attached_to_doctype", "attached_to_name"],
        limit=10  # Limit to 10 for performance
    )
    
    if referencing_files:
        # Build error message with details
        ref_details = []
        for ref in referencing_files[:5]:  # Show max 5 in error
            if ref.attached_to_doctype and ref.attached_to_name:
                ref_details.append(
                    f"• File {ref.name} (attached to {ref.attached_to_doctype}: {ref.attached_to_name})"
                )
            else:
                ref_details.append(f"• File {ref.name}")
        
        total_refs = len(referencing_files)
        if total_refs > 5:
            ref_details.append(f"... and {total_refs - 5} more")
        
        error_msg = _(
            "Cannot delete this file because it is referenced by {0} other file(s):\n\n{1}\n\n"
            "Please delete the referencing files first, or delete the documents they are attached to."
        ).format(total_refs, "\n".join(ref_details))
        
        frappe.throw(error_msg, title=_("File is Referenced"))


def hook_file_after_delete(doc, method):
    """
    Called after a File document is deleted.
    
    Deletes the S3 object only if no other File documents reference it.
    Reference counting is handled in dfp_external_storage_delete_file().
    """
    doc.dfp_external_storage_delete_file()


class DFPExternalStorageFileRenderer:
    def __init__(self, path, status_code=None):
        self.path = path
        self.status_code = status_code
        self._regex = None

    def _regexed_path(self):
        self._regex = re.search(
            rf"{DFP_EXTERNAL_STORAGE_URL_SEGMENT_FOR_FILE_LOAD}\/(.+)\/(.+\.\w+)$",
            self.path,
        )

    def file_id_get(self):
        if self.can_render():
            return self._regex[1]

    def can_render(self):
        if not self._regex:
            self._regexed_path()
        if self._regex:
            return True

    def render(self):
        file_id = self._regex[1]
        file_name = self._regex[2] if len(self._regex.regs) == 3 else ""
        return file(name=file_id, file=file_name)


def file(name: str, file: str):
    if not name or not file:
        raise frappe.PageDoesNotExistError()

    cache_key = f"{DFP_EXTERNAL_STORAGE_PUBLIC_CACHE_PREFIX}{name}"

    response_values = frappe.cache().get_value(cache_key)
    if not response_values:
        try:
            doc = frappe.get_doc("File", name)
            if not doc or not doc.is_downloadable() or doc.file_name != file:
                raise Exception("File not available")
        except Exception:
            # If no document, no read permissions, etc. For security reasons do not give any information, so just raise a 404 error
            raise frappe.PageDoesNotExistError()

        response_values = {}
        response_values["headers"] = []

        try:
            presigned_url = doc.dfp_presigned_url_get()
            if presigned_url:
                frappe.flags.redirect_location = presigned_url
                raise frappe.Redirect
            # Do not stream file if cacheable or smaller than stream buffer chunks size
            if (
                doc.dfp_is_cacheable()
                or doc.dfp_file_size
                < doc.dfp_external_storage_doc.setting_stream_buffer_size
            ):
                response_values["response"] = doc.dfp_external_storage_download_file()
            else:
                response_values["response"] = doc.dfp_external_storage_stream_file()
                response_values["headers"].append(("Content-Length", doc.dfp_file_size))
        except frappe.Redirect:
            raise
        except:
            frappe.log_error(f"Error obtaining remote file content: {name}/{file}")

        if "response" not in response_values or not response_values["response"]:
            raise frappe.PageDoesNotExistError()

        if doc.dfp_mime_type_guess_by_file_name:
            response_values["mimetype"] = doc.dfp_mime_type_guess_by_file_name
        response_values["status"] = 200

        if doc.dfp_is_cacheable():
            frappe.cache().set_value(
                key=cache_key,
                val=response_values,
                expires_in_sec=doc.dfp_external_storage_doc.setting_cache_expiration_secs,
            )

    if "status" in response_values and response_values["status"] == 200:
        return Response(**response_values)

    raise frappe.PageDoesNotExistError()


def is_url(string):
    try:
        result = urlparse(string)
        # Check if scheme and netloc are present
        return all([result.scheme, result.netloc])
    except ValueError:
        return False
