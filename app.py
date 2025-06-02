from flask import Flask, jsonify, request
import logging
import traceback
import sys
import os
import time
from typing import Dict, Any, Optional
from datetime import datetime
import threading
import time
from datetime import datetime
import schedule
from pathlib import Path

# Add after the existing imports
monitoring_thread = None
monitoring_active = False

# Configure logging with better formatting and rotation
from logging.handlers import RotatingFileHandler

def setup_logging():
    """Setup production-grade logging with rotation."""
    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)

    # File handler with rotation
    file_handler = RotatingFileHandler(
        'logs/email_processor.log',
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    file_handler.setFormatter(file_formatter)

    # Add handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logging.getLogger(__name__)

logger = setup_logging()

app = Flask(__name__)

# Flask app configuration
app.config.update(
    # Security: Secret key for session signing and CSRF protection
    SECRET_KEY=os.environ.get('SECRET_KEY', 'production-secret-key-change-me-in-production'),

    # JSON settings: Don't sort keys (faster), don't pretty print (smaller response)
    JSON_SORT_KEYS=False,
    JSONIFY_PRETTYPRINT_REGULAR=False,

    # Additional production settings
    TESTING=False,
    PROPAGATE_EXCEPTIONS=True
)

# Global variables for module imports
CheckEmailOutstanding = None
OpenFileWithHiddenColumns = None
SendAttachmentModule = None
_modules_imported = False
_import_lock = threading.Lock()

def import_modules() -> bool:
    """Thread-safe lazy import of modules."""
    global CheckEmailOutstanding, OpenFileWithHiddenColumns, SendAttachmentModule, _modules_imported

    if _modules_imported:
        return True

    with _import_lock:
        if _modules_imported:  # Double-check pattern
            return True

        try:
            logger.info("Importing required modules...")

            # Import with proper error handling
            try:
                import CheckEmailOutstanding as _CheckEmailOutstanding
                CheckEmailOutstanding = _CheckEmailOutstanding

                # Patch signal handling in the imported module
                patch_signal_handling_in_module(CheckEmailOutstanding)

                logger.info("✅ CheckEmailOutstanding imported successfully")
            except ImportError as e:
                logger.error(f"❌ Failed to import CheckEmailOutstanding: {e}")
                return False

            try:
                import OpenFileWithHiddenColumns as _OpenFileWithHiddenColumns
                OpenFileWithHiddenColumns = _OpenFileWithHiddenColumns
                logger.info("✅ OpenFileWithHiddenColumns imported successfully")
            except ImportError as e:
                logger.error(f"❌ Failed to import OpenFileWithHiddenColumns: {e}")
                return False

            # Handle module with spaces in name
            import importlib.util
            import sys

            # Try different possible module names
            possible_names = [
                'Send Attachment to Export and Noida',
                'Send_Attachment_to_Export_and_Noida',
                'SendAttachmentToExportAndNoida'
            ]

            SendAttachmentModule = None
            for module_name in possible_names:
                try:
                    SendAttachmentModule = importlib.import_module(module_name)
                    logger.info(f"✅ Successfully imported email module: {module_name}")
                    break
                except ImportError:
                    continue

            if SendAttachmentModule is None:
                logger.warning("⚠️ Could not import Send Attachment module - email sending will be disabled")

            _modules_imported = True
            logger.info("🎉 All required modules imported successfully")
            return True

        except Exception as e:
            logger.error(f"💥 Critical import error: {e}")
            logger.error(traceback.format_exc())
            return False

def patch_signal_handling_in_module(module):
    """Patch signal handling in an imported module to make it thread-safe."""
    try:
        # Look for SterlingEmailMonitor class
        if hasattr(module, 'SterlingEmailMonitor'):
            monitor_class = getattr(module, 'SterlingEmailMonitor')

            # Patch the _setup_signal_handlers method
            if hasattr(monitor_class, '_setup_signal_handlers'):
                original_method = monitor_class._setup_signal_handlers

                def thread_safe_setup_signal_handlers(self):
                    """Thread-safe version of signal handler setup."""
                    if threading.current_thread() is not threading.main_thread():
                        logger.debug("Skipping signal handler setup - not in main thread")
                        return
                    try:
                        return original_method(self)
                    except Exception as e:
                        logger.debug(f"Signal handler setup failed (expected in threads): {e}")
                        return

                monitor_class._setup_signal_handlers = thread_safe_setup_signal_handlers
                logger.info("✅ Patched _setup_signal_handlers in SterlingEmailMonitor")

        # Also patch any global signal setup in the module
        if hasattr(module, 'signal'):
            import signal as signal_module
            original_signal = signal_module.signal

            def thread_safe_signal(sig, handler):
                """Thread-safe signal setup."""
                if threading.current_thread() is not threading.main_thread():
                    logger.debug(f"Ignoring signal {sig} setup in thread")
                    return None
                return original_signal(sig, handler)

            # Replace signal.signal in the module's namespace if it's imported there
            if hasattr(module, 'signal') and hasattr(module.signal, 'signal'):
                module.signal.signal = thread_safe_signal
                logger.info("✅ Patched signal.signal in module namespace")

    except Exception as e:
        logger.warning(f"Could not patch signal handling in module: {e}")
        # Continue anyway - the runtime patching might still work

def find_latest_excel_file(directory: str, filename_hint: str = None) -> Optional[str]:
    """Find the latest Excel file, with smart matching for Sterling files."""
    try:
        dir_path = Path(directory)
        if not dir_path.exists():
            logger.warning(f"Directory does not exist: {directory}")
            return None

        # Get all Excel files
        xlsx_files = list(dir_path.glob("*.xlsx"))

        if not xlsx_files:
            logger.warning(f"No Excel files found in {directory}")
            return None

        # If we have a filename hint, try to find exact match first
        if filename_hint:
            hint_path = dir_path / filename_hint
            if hint_path.exists():
                logger.info(f"Found exact file match: {hint_path}")
                return str(hint_path)

        # Smart matching for Sterling files - look for key patterns
        sterling_patterns = [
            "STERLING outstanding order list",
            "STERLING ORNAMENTS outstanding order list",
            "STERLING",
            "outstanding order list",
            "sterling_"  # For processed filenames with prefixes
        ]

        # Try to find Sterling-related files
        sterling_files = []
        for xlsx_file in xlsx_files:
            filename_lower = xlsx_file.name.lower()
            if any(pattern.lower() in filename_lower for pattern in sterling_patterns):
                sterling_files.append(xlsx_file)
                logger.debug(f"Found Sterling file: {xlsx_file.name}")

        if sterling_files:
            # Get the most recent Sterling file
            latest_sterling = max(sterling_files, key=lambda f: f.stat().st_mtime)
            logger.info(f"Found latest Sterling file: {latest_sterling.name}")
            return str(latest_sterling)

        # If filename hint provided, try partial matching
        if filename_hint:
            hint_parts = [part for part in filename_hint.split() if len(part) > 3]
            for xlsx_file in xlsx_files:
                if any(part.lower() in xlsx_file.name.lower() for part in hint_parts):
                    logger.info(f"Found partial match: {xlsx_file.name}")
                    return str(xlsx_file)

        # Get the most recent file as final fallback
        latest_file = max(xlsx_files, key=lambda f: f.stat().st_mtime)
        logger.info(f"Using most recent Excel file as fallback: {latest_file.name}")
        return str(latest_file)

    except Exception as e:
        logger.error(f"Error finding Excel file: {str(e)}")
        return None

def validate_file_path(file_path: str) -> Dict[str, Any]:
    """Enhanced file path validation with better fallback logic."""
    if not file_path:
        return {
            'status': 'error',
            'message': 'No file path provided'
        }

    file_path = Path(file_path)

    # Check if file exists
    if file_path.exists():
        logger.info(f"File found: {file_path}")
        return {
            'status': 'success',
            'file_path': str(file_path)
        }

    # Debug: List all files in the directory
    directory = file_path.parent
    filename = file_path.name

    logger.info(f"File not found at {file_path}")
    logger.info(f"Searching in directory: {directory}")

    # List all files for debugging
    try:
        all_files = list(directory.glob("*"))
        logger.info(f"All files in directory ({len(all_files)}): {[f.name for f in all_files[:10]]}")  # Show first 10

        xlsx_files = list(directory.glob("*.xlsx"))
        logger.info(f"Excel files found ({len(xlsx_files)}): {[f.name for f in xlsx_files]}")
    except Exception as e:
        logger.error(f"Error listing directory contents: {e}")

    # Search for the file with improved logic
    found_file = find_latest_excel_file(str(directory), filename)

    if found_file:
        return {
            'status': 'success',
            'file_path': found_file,
            'message': f'Original file not found, using: {Path(found_file).name}'
        }

    return {
        'status': 'error',
        'message': f'No valid Excel files found in {directory}. Expected: {filename}'
    }

def check_for_no_emails_found(email_result: Dict[str, Any]) -> bool:
    """Check if the email result indicates no emails were found."""
    if not isinstance(email_result, dict):
        return False

    message = email_result.get('message', '').lower()

    no_email_indicators = [
        'no new sterling emails found',
        'no emails found',
        'no new emails',
        'no sterling emails',
        'no emails to process',
        'most recent email already processed',
        'no new sterling ornaments emails found',  # Added for ornaments variant
        'already processed'
    ]

    return any(indicator in message for indicator in no_email_indicators)

def continuous_email_monitor():
    """Background thread for continuous email monitoring."""
    global monitoring_active

    logger.info("🔄 Starting continuous email monitoring")

    while monitoring_active:
        try:
            logger.info("📧 Running scheduled email check...")
            result = process_email_pipeline(single_run=True, verbose=False, send_email=True)

            if result.get('no_emails_found'):
                logger.debug("No new emails found in scheduled check")
            else:
                logger.info(f"Scheduled check result: {result.get('message')}")

        except Exception as e:
            logger.error(f"Error in continuous monitoring: {e}")

        # Wait for next check (configurable interval)
        check_interval = int(os.environ.get('CHECK_INTERVAL_MINUTES', '5'))
        time.sleep(check_interval * 60)

    logger.info("🛑 Continuous email monitoring stopped")

def start_monitoring():
    """Start background email monitoring."""
    global monitoring_thread, monitoring_active

    if monitoring_active:
        return {'status': 'already_running', 'message': 'Monitoring already active'}

    monitoring_active = True
    monitoring_thread = threading.Thread(target=continuous_email_monitor, daemon=True)
    monitoring_thread.start()

    logger.info("✅ Background email monitoring started")
    return {'status': 'started', 'message': 'Background monitoring started'}

def stop_monitoring():
    """Stop background email monitoring."""
    global monitoring_active

    if not monitoring_active:
        return {'status': 'not_running', 'message': 'Monitoring not active'}

    monitoring_active = False
    logger.info("🛑 Background email monitoring stopped")
    return {'status': 'stopped', 'message': 'Background monitoring stopped'}

def call_email_module_safely(single_run=True, verbose=True):
    """Call CheckEmailOutstanding in a way that avoids signal issues."""
    try:
        # Method 1: Monkey-patch the signal handling in the imported module
        import signal
        original_signal = signal.signal

        def dummy_signal(sig, handler):
            """Dummy signal handler that does nothing in threads."""
            if threading.current_thread() is not threading.main_thread():
                logger.debug(f"Ignoring signal {sig} setup in thread {threading.current_thread().name}")
                return None  # Return None instead of calling original
            return original_signal(sig, handler)

        # Method 2: Also patch the _setup_signal_handlers method if it exists
        original_setup_signal_handlers = None
        if hasattr(CheckEmailOutstanding, 'SterlingEmailMonitor'):
            monitor_class = getattr(CheckEmailOutstanding, 'SterlingEmailMonitor')
            if hasattr(monitor_class, '_setup_signal_handlers'):
                original_setup_signal_handlers = monitor_class._setup_signal_handlers

                def dummy_setup_signal_handlers(self):
                    """Dummy signal setup that does nothing in threads."""
                    if threading.current_thread() is not threading.main_thread():
                        logger.debug("Skipping signal handler setup in thread")
                        return
                    return original_setup_signal_handlers(self)

                monitor_class._setup_signal_handlers = dummy_setup_signal_handlers

        # Temporarily replace signal.signal
        signal.signal = dummy_signal

        try:
            logger.info("Calling CheckEmailOutstanding.main with thread-safe signal handling")

            # Check if main function exists and call it
            if hasattr(CheckEmailOutstanding, 'main') and callable(getattr(CheckEmailOutstanding, 'main')):
                result = CheckEmailOutstanding.main(single_run=single_run, verbose=verbose)
            else:
                # Alternative: try to create and use the monitor directly
                logger.info("main function not found, trying to use SterlingEmailMonitor directly")
                if hasattr(CheckEmailOutstanding, 'SterlingEmailMonitor'):
                    monitor = CheckEmailOutstanding.SterlingEmailMonitor()
                    if single_run:
                        monitor.run_scheduled_check()

                        # Try to get the result in the expected format
                        all_emails_summary = monitor.get_all_processed_emails_summary()

                        # Try to find the latest email
                        latest_email_data = None
                        xlsx_path = None
                        all_attachments = []

                        if monitor.file_manager.emails_dir.exists():
                            json_files = list(monitor.file_manager.emails_dir.glob('sterling_email_*.json'))
                            if json_files:
                                latest_json_file = max(json_files, key=lambda x: x.stat().st_mtime)
                                latest_email_data = monitor.file_manager.load_json(latest_json_file)

                                if latest_email_data and latest_email_data.get('attachments'):
                                    for attachment in latest_email_data['attachments']:
                                        full_path = Path(attachment['saved_path']).resolve()
                                        all_attachments.append(str(full_path))

                                        if attachment['original_name'].lower().endswith('.xlsx') and xlsx_path is None:
                                            xlsx_path = str(full_path)

                        if latest_email_data:
                            result = {
                                'status': 'success',
                                'message': f"Successfully processed Sterling email (Retrieved at: {latest_email_data.get('retrieved_at')})",
                                'output_file': xlsx_path or (all_attachments[0] if all_attachments else latest_email_data.get('email_content_file')),
                                'email_data': latest_email_data,
                                'all_attachments': all_attachments,
                                'all_emails_summary': all_emails_summary
                            }
                        else:
                            result = {
                                'status': 'success',
                                'message': "No new Sterling emails found.",
                                'output_file': None,
                                'email_data': None,
                                'all_attachments': [],
                                'all_emails_summary': all_emails_summary
                            }
                    else:
                        monitor.start_monitoring()
                        result = {
                            'status': 'success',
                            'message': "Monitoring started (continuous mode)",
                            'output_file': None,
                            'email_data': None,
                            'all_attachments': [],
                            'all_emails_summary': {}
                        }
                else:
                    raise AttributeError("Neither 'main' function nor 'SterlingEmailMonitor' class found in CheckEmailOutstanding module")

            logger.info("CheckEmailOutstanding processing completed successfully")
            return result
        finally:
            # Restore original functions
            signal.signal = original_signal
            if original_setup_signal_handlers and hasattr(CheckEmailOutstanding, 'SterlingEmailMonitor'):
                monitor_class = getattr(CheckEmailOutstanding, 'SterlingEmailMonitor')
                if hasattr(monitor_class, '_setup_signal_handlers'):
                    monitor_class._setup_signal_handlers = original_setup_signal_handlers

    except Exception as e:
        logger.error(f"Error in thread-safe email call: {e}")
        logger.error(traceback.format_exc())
        return {
            'status': 'error',
            'message': f'Email processing failed: {str(e)}',
            'error_details': str(e),
            'email_data': None,
            'all_attachments': [],
            'all_emails_summary': {},
            'output_file': None
        }
    """Safely call a function with comprehensive error handling."""
    func_name = getattr(func, '__name__', str(func))

    try:
        logger.info(f"Calling {func_name} with args: {args}, kwargs: {kwargs}")

        # Special handling for functions that might use signal handling
        if 'main' in func_name.lower():
            logger.warning(f"Calling {func_name} - this function may use signal handling")

        result = func(*args, **kwargs)

        # Ensure result is a dictionary
        if not isinstance(result, dict):
            return {
                'status': 'success',
                'message': str(result),
                'raw_result': result
            }

        return result

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error calling {func_name}: {error_msg}")
        logger.error(traceback.format_exc())

        # Special handling for signal-related errors
        if "signal only works in main thread" in error_msg:
            logger.error(f"SIGNAL ERROR: {func_name} is trying to use signal handling in a thread!")
            logger.error("This is likely in your CheckEmailOutstanding.py or other modules")
            logger.error("You need to remove signal handling from these modules when called from Flask")

            return {
                'status': 'error',
                'message': f'Signal error in {func_name}: {error_msg}',
                'error_details': str(e),
                'fix_needed': f'Remove signal handling from {func_name} or modify it to work in threads'
            }

        return {
            'status': 'error',
            'message': f'Error in {func_name}: {error_msg}',
            'error_details': str(e)
        }

def process_email_pipeline(single_run: bool = True, verbose: bool = True, send_email: bool = True) -> Dict[str, Any]:
    """Production-ready email processing pipeline."""
    pipeline_start = datetime.now()

    try:
        logger.info("=" * 60)
        logger.info("STARTING EMAIL PROCESSING PIPELINE")
        logger.info(f"Current thread: {threading.current_thread().name}")
        logger.info(f"Parameters: single_run={single_run}, verbose={verbose}, send_email={send_email}")
        logger.info("=" * 60)

        # Import modules
        if not import_modules():
            return {
                'status': 'error',
                'message': 'Failed to import required modules',
                'timestamp': pipeline_start.isoformat()
            }

        # Step 1: Email Processing
        logger.info("STEP 1: Email Processing")
        email_start = datetime.now()

        # Always use the thread-safe wrapper for email processing
        email_result = call_email_module_safely(single_run=single_run, verbose=verbose)

        email_duration = (datetime.now() - email_start).total_seconds()
        logger.info(f"Email processing completed in {email_duration:.2f}s")

        # Handle no emails found case
        if check_for_no_emails_found(email_result):
            logger.info("No new emails found - pipeline completed successfully")
            return {
                'status': 'success',
                'message': 'No new Sterling emails found during this check',
                'email_result': email_result,
                'timestamp': pipeline_start.isoformat(),
                'duration': {
                    'total': email_duration,
                    'email_processing': email_duration,
                    'file_processing': 0,
                    'email_sending': 0
                },
                'no_emails_found': True
            }

        # Check email processing status
        if email_result.get('status') == 'error':
            logger.error(f"Email processing failed: {email_result.get('message')}")
            return {
                **email_result,
                'timestamp': pipeline_start.isoformat(),
                'duration': {'email_processing': email_duration}
            }

        # Step 2: File Validation
        logger.info("STEP 2: File Validation")
        output_file = email_result.get('output_file')

        if not output_file:
            return {
                'status': 'error',
                'message': 'No output file returned from email processing',
                'email_result': email_result,
                'timestamp': pipeline_start.isoformat()
            }

        file_validation = validate_file_path(output_file)
        if file_validation['status'] != 'success':
            logger.error(f"File validation failed: {file_validation['message']}")
            return {
                **file_validation,
                'email_result': email_result,
                'timestamp': pipeline_start.isoformat()
            }

        validated_file_path = file_validation['file_path']
        logger.info(f"Validated file path: {validated_file_path}")

        # Step 3: File Processing
        logger.info("STEP 3: File Processing")
        file_start = datetime.now()

        try:
            logger.info(f"Calling OpenFileWithHiddenColumns.main_with_option with file: {validated_file_path}")
            file_result = OpenFileWithHiddenColumns.main_with_option(
                input_file=validated_file_path,
                simple_return=True
            )

            # Ensure result is a dictionary
            if not isinstance(file_result, dict):
                file_result = {
                    'status': 'success',
                    'message': str(file_result),
                    'raw_result': file_result,
                    'output_file': validated_file_path  # Fallback to input file
                }
        except Exception as e:
            logger.error(f"Error in file processing: {str(e)}")
            logger.error(traceback.format_exc())
            file_result = {
                'status': 'error',
                'message': f'Error in file processing: {str(e)}',
                'error_details': str(e)
            }

        file_duration = (datetime.now() - file_start).total_seconds()
        logger.info(f"File processing completed in {file_duration:.2f}s")

        if file_result.get('status') == 'error':
            logger.error(f"File processing failed: {file_result.get('message')}")
            return {
                **file_result,
                'email_result': email_result,
                'timestamp': pipeline_start.isoformat(),
                'duration': {
                    'email_processing': email_duration,
                    'file_processing': file_duration
                }
            }

        # Step 4: Email Sending
        email_send_result = {'status': 'skipped', 'message': 'Email sending disabled'}
        email_send_duration = 0

        if send_email and SendAttachmentModule:
            logger.info("STEP 4: Email Sending")

            processed_file = file_result.get('output_file', validated_file_path)
            logger.info(f"Sending email with file: {processed_file}")

            email_send_start = datetime.now()
            try:
                logger.info(f"Calling SendAttachmentModule.send_sterling_email with file: {processed_file}")
                email_send_result = SendAttachmentModule.send_sterling_email(processed_file)

                # Ensure result is a dictionary
                if not isinstance(email_send_result, dict):
                    email_send_result = {
                        'status': 'success',
                        'message': str(email_send_result),
                        'raw_result': email_send_result
                    }
            except Exception as e:
                logger.error(f"Error in email sending: {str(e)}")
                logger.error(traceback.format_exc())
                email_send_result = {
                    'status': 'error',
                    'message': f'Error in email sending: {str(e)}',
                    'error_details': str(e)
                }

            email_send_duration = (datetime.now() - email_send_start).total_seconds()
            logger.info(f"Email sending completed in {email_send_duration:.2f}s")

        elif send_email and not SendAttachmentModule:
            logger.warning("Email sending requested but module not available")
            email_send_result = {
                'status': 'error',
                'message': 'Email module not available'
            }
        else:
            logger.info("STEP 4: Email sending skipped (disabled)")

        # Final Results
        total_duration = (datetime.now() - pipeline_start).total_seconds()

        result = {
            'status': 'success',
            'message': 'Pipeline completed successfully',
            'email_result': email_result,
            'file_result': file_result,
            'email_send_result': email_send_result,
            'final_output': file_result.get('output_file'),
            'timestamp': pipeline_start.isoformat(),
            'duration': {
                'total': total_duration,
                'email_processing': email_duration,
                'file_processing': file_duration,
                'email_sending': email_send_duration
            },
            'no_emails_found': False,
            'thread_info': {
                'thread_name': threading.current_thread().name
            }
        }

        if file_validation.get('message'):
            result['file_path_note'] = file_validation['message']

        logger.info("=" * 60)
        logger.info(f"PIPELINE COMPLETED SUCCESSFULLY in {total_duration:.2f}s")
        logger.info("=" * 60)

        return result

    except Exception as e:
        total_duration = (datetime.now() - pipeline_start).total_seconds()
        error_msg = f"Pipeline failed after {total_duration:.2f}s: {str(e)}"

        logger.error(error_msg)
        logger.error(traceback.format_exc())

        return {
            'status': 'error',
            'message': error_msg,
            'timestamp': pipeline_start.isoformat(),
            'duration': total_duration,
            'error_details': str(e),
            'thread_info': {
                'thread_name': threading.current_thread().name
            }
        }

# API Routes
@app.route('/health', methods=['GET'])
def health_check():
    """Comprehensive health check endpoint."""
    try:
        start_time = time.time()

        # Check module availability
        modules_available = import_modules()

        # Check disk space
        try:
            disk_usage = os.statvfs('.')
            free_space_mb = (disk_usage.f_frsize * disk_usage.f_bavail) / (1024 * 1024)
        except (OSError, AttributeError):
            free_space_mb = "unavailable"

        health_data = {
            'status': 'healthy',
            'service': 'email-processor',
            'version': '1.0.0',
            'timestamp': datetime.now().isoformat(),
            'modules_available': modules_available,
            'email_module_available': SendAttachmentModule is not None,
            'python_version': sys.version,
            'working_directory': os.getcwd(),
            'free_disk_space_mb': free_space_mb if isinstance(free_space_mb, str) else round(free_space_mb, 2),
            'response_time_ms': round((time.time() - start_time) * 1000, 2),
            'thread_info': {
                'current_thread': threading.current_thread().name,
                'active_threads': threading.active_count()
            }
        }

        return jsonify(health_data)

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/process', methods=['POST'])
def process_emails():
    """Process emails with JSON configuration."""
    request_start = time.time()

    try:
        # Validate and parse request
        if not request.is_json:
            return jsonify({
                'status': 'error',
                'message': 'Content-Type must be application/json'
            }), 400

        data = request.get_json() or {}

        # Extract and validate parameters
        single_run = bool(data.get('single_run', True))
        verbose = bool(data.get('verbose', True))
        send_email = bool(data.get('send_email', True))

        logger.info(f"POST /process - params: single_run={single_run}, verbose={verbose}, send_email={send_email}")

        # Process pipeline
        result = process_email_pipeline(
            single_run=single_run,
            verbose=verbose,
            send_email=send_email
        )

        # Add request metadata
        result['request_time_ms'] = round((time.time() - request_start) * 1000, 2)
        result['api_version'] = '1.0.0'

        status_code = 200 if result['status'] == 'success' else 500
        return jsonify(result), status_code

    except Exception as e:
        logger.error(f"API error in POST /process: {str(e)}")
        logger.error(traceback.format_exc())

        return jsonify({
            'status': 'error',
            'message': f'API error: {str(e)}',
            'timestamp': datetime.now().isoformat(),
            'request_time_ms': round((time.time() - request_start) * 1000, 2)
        }), 500

@app.route('/process', methods=['GET'])
def process_emails_get():
    """Simple GET endpoint with default parameters."""
    try:
        logger.info("GET /process - using default parameters")
        result = process_email_pipeline()
        status_code = 200 if result['status'] == 'success' else 500
        return jsonify(result), status_code

    except Exception as e:
        logger.error(f"API error in GET /process: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'API error: {str(e)}',
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/monitor/start', methods=['POST'])
def start_background_monitoring():
    """Start continuous background email monitoring."""
    try:
        data = request.get_json() or {}
        interval_minutes = data.get('interval_minutes', 5)

        # Set environment variable for interval
        os.environ['CHECK_INTERVAL_MINUTES'] = str(interval_minutes)

        result = start_monitoring()
        result['interval_minutes'] = interval_minutes
        result['timestamp'] = datetime.now().isoformat()

        return jsonify(result)

    except Exception as e:
        logger.error(f"Error starting monitoring: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to start monitoring: {str(e)}',
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/monitor/stop', methods=['POST'])
def stop_background_monitoring():
    """Stop continuous background email monitoring."""
    try:
        result = stop_monitoring()
        result['timestamp'] = datetime.now().isoformat()
        return jsonify(result)

    except Exception as e:
        logger.error(f"Error stopping monitoring: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to stop monitoring: {str(e)}',
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/monitor/status', methods=['GET'])
def monitoring_status():
    """Get current monitoring status."""
    try:
        return jsonify({
            'status': 'active' if monitoring_active else 'inactive',
            'monitoring_active': monitoring_active,
            'check_interval_minutes': int(os.environ.get('CHECK_INTERVAL_MINUTES', '5')),
            'thread_alive': monitoring_thread.is_alive() if monitoring_thread else False,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/debug/inspect-modules', methods=['GET'])
def inspect_modules():
    """Inspect the imported modules to see what's available."""
    results = {}

    try:
        if CheckEmailOutstanding:
            # Get all attributes of the CheckEmailOutstanding module
            attributes = dir(CheckEmailOutstanding)

            # Categorize attributes
            functions = []
            classes = []
            other = []

            for attr in attributes:
                if not attr.startswith('_'):  # Skip private attributes
                    obj = getattr(CheckEmailOutstanding, attr)
                    if callable(obj):
                        if hasattr(obj, '__class__') and 'function' in str(obj.__class__):
                            functions.append(attr)
                        elif hasattr(obj, '__class__') and 'type' in str(obj.__class__):
                            classes.append(attr)
                        else:
                            functions.append(attr)
                    else:
                        other.append(attr)

            results['CheckEmailOutstanding'] = {
                'status': 'imported',
                'functions': functions,
                'classes': classes,
                'other_attributes': other,
                'has_main': hasattr(CheckEmailOutstanding, 'main'),
                'main_callable': hasattr(CheckEmailOutstanding, 'main') and callable(getattr(CheckEmailOutstanding, 'main', None)),
                'has_SterlingEmailMonitor': hasattr(CheckEmailOutstanding, 'SterlingEmailMonitor')
            }

            # If SterlingEmailMonitor exists, inspect it too
            if hasattr(CheckEmailOutstanding, 'SterlingEmailMonitor'):
                monitor_class = getattr(CheckEmailOutstanding, 'SterlingEmailMonitor')
                monitor_methods = [attr for attr in dir(monitor_class) if not attr.startswith('_') and callable(getattr(monitor_class, attr))]
                results['SterlingEmailMonitor'] = {
                    'methods': monitor_methods,
                    'has_setup_signal_handlers': hasattr(monitor_class, '_setup_signal_handlers')
                }
        else:
            results['CheckEmailOutstanding'] = {'status': 'not_imported'}

        # Also check if we can find the main function at module level
        try:
            if CheckEmailOutstanding:
                # Try to call main with inspection first
                main_func = getattr(CheckEmailOutstanding, 'main', None)
                if main_func:
                    import inspect
                    signature = inspect.signature(main_func)
                    results['main_function_signature'] = str(signature)
                else:
                    results['main_function_signature'] = 'main function not found'
        except Exception as e:
            results['main_function_inspection_error'] = str(e)

        return jsonify({
            'status': 'success',
            'timestamp': datetime.now().isoformat(),
            'module_inspection': results
        })

    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500
def test_modules():
    """Test each module individually to identify signal issues."""
    results = {}

    try:
        # Test CheckEmailOutstanding
        if CheckEmailOutstanding:
            try:
                logger.info("Testing CheckEmailOutstanding module...")
                # Try calling with minimal parameters first
                result = CheckEmailOutstanding.main(single_run=True, verbose=False)
                results['CheckEmailOutstanding'] = {
                    'status': 'success',
                    'result': str(result)[:200] + "..." if len(str(result)) > 200 else str(result)
                }
            except Exception as e:
                results['CheckEmailOutstanding'] = {
                    'status': 'error',
                    'error': str(e),
                    'is_signal_error': 'signal only works in main thread' in str(e)
                }
        else:
            results['CheckEmailOutstanding'] = {'status': 'not_imported'}

        # Test OpenFileWithHiddenColumns
        if OpenFileWithHiddenColumns:
            try:
                # This one probably won't have signal issues, but let's test anyway
                results['OpenFileWithHiddenColumns'] = {
                    'status': 'available',
                    'functions': [attr for attr in dir(OpenFileWithHiddenColumns) if not attr.startswith('_')]
                }
            except Exception as e:
                results['OpenFileWithHiddenColumns'] = {
                    'status': 'error',
                    'error': str(e)
                }
        else:
            results['OpenFileWithHiddenColumns'] = {'status': 'not_imported'}

        # Test SendAttachmentModule
        if SendAttachmentModule:
            try:
                results['SendAttachmentModule'] = {
                    'status': 'available',
                    'functions': [attr for attr in dir(SendAttachmentModule) if not attr.startswith('_')]
                }
            except Exception as e:
                results['SendAttachmentModule'] = {
                    'status': 'error',
                    'error': str(e)
                }
        else:
            results['SendAttachmentModule'] = {'status': 'not_imported'}

        return jsonify({
            'status': 'success',
            'timestamp': datetime.now().isoformat(),
            'module_tests': results,
            'thread_info': {
                'current_thread': threading.current_thread().name,
                'is_main_thread': threading.current_thread() is threading.main_thread()
            }
        })

    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500
def debug_files():
    """Debug endpoint to list files in attachments directory."""
    try:
        # Common attachment directories to check
        possible_dirs = [
            'sterling_emails/attachments',
            'attachments',
            'downloads',
            '.'
        ]

        results = {}

        for dir_path in possible_dirs:
            path = Path(dir_path)
            if path.exists():
                try:
                    all_files = list(path.glob("*"))
                    xlsx_files = list(path.glob("*.xlsx"))

                    results[str(path)] = {
                        'exists': True,
                        'total_files': len(all_files),
                        'xlsx_files': len(xlsx_files),
                        'xlsx_list': [f.name for f in xlsx_files],
                        'recent_files': [f.name for f in sorted(all_files, key=lambda x: x.stat().st_mtime, reverse=True)[:5]]
                    }
                except Exception as e:
                    results[str(path)] = {
                        'exists': True,
                        'error': str(e)
                    }
            else:
                results[str(path)] = {
                    'exists': False
                }

        return jsonify({
            'status': 'success',
            'timestamp': datetime.now().isoformat(),
            'directories': results,
            'cwd': os.getcwd()
        })

    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/status', methods=['GET'])
def system_status():
    """System status with recent logs."""
    try:
        # Read recent logs
        recent_logs = []
        log_file = Path('logs/email_processor.log')

        if log_file.exists():
            try:
                with open(log_file, 'r') as f:
                    lines = f.readlines()
                    recent_logs = [line.strip() for line in lines[-50:]]  # Last 50 lines
            except Exception as e:
                recent_logs = [f"Error reading log file: {e}"]
        else:
            recent_logs = ["Log file not found"]

        return jsonify({
            'status': 'active',
            'timestamp': datetime.now().isoformat(),
            'recent_logs': recent_logs,
            'log_count': len(recent_logs),
            'thread_info': {
                'current_thread': threading.current_thread().name,
                'active_threads': threading.active_count()
            },
            'endpoints': {
                'health': '/health - System health check',
                'process_post': '/process (POST) - Process with parameters',
                'process_get': '/process (GET) - Process with defaults',
                'status': '/status - Current status and logs',
                'monitor_start': '/monitor/start (POST) - Start continuous monitoring',
                'monitor_stop': '/monitor/stop (POST) - Stop continuous monitoring',
                'monitor_status': '/monitor/status (GET) - Monitoring status',
                'debug_files': '/debug/files (GET) - File system debug'
            }
        })

    except Exception as e:
        logger.error(f"Status endpoint error: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

# Error Handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'status': 'error',
        'message': 'Endpoint not found',
        'available_endpoints': ['/health', '/process', '/status'],
        'timestamp': datetime.now().isoformat()
    }), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {str(error)}")
    return jsonify({
        'status': 'error',
        'message': 'Internal server error',
        'timestamp': datetime.now().isoformat()
    }), 500

@app.errorhandler(400)
def bad_request(error):
    return jsonify({
        'status': 'error',
        'message': 'Bad request',
        'timestamp': datetime.now().isoformat()
    }), 400

# WSGI Application Factory
def create_app():
    """Application factory for WSGI deployment."""
    # Import modules on startup
    if import_modules():
        logger.info("✅ All modules imported successfully")
    else:
        logger.warning("⚠️  Some modules failed to import - functionality may be limited")

    logger.info("🚀 Email Processor API initialized for WSGI")
    return app

# Development server startup
def run_development_server():
    """Run server with development configuration."""
    port = int(os.environ.get('PORT', 8080))
    host = os.environ.get('HOST', '0.0.0.0')
    debug_mode = os.environ.get('DEBUG', 'True').lower() == 'true'

    print("=" * 80)
    print("🚀 STARTING EMAIL PROCESSOR API (DEVELOPMENT)")
    print("=" * 80)
    print(f"📡 Server: http://{host}:{port}")
    print(f"🏥 Health: http://{host}:{port}/health")
    print(f"⚙️  Process: http://{host}:{port}/process")
    print(f"📊 Status: http://{host}:{port}/status")
    print(f"🐛 Debug: {debug_mode}")
    print(f"🗂️  Logs: logs/email_processor.log")
    print("⚠️  WARNING: Using development server!")
    print("⚠️  For production, use: gunicorn -c gunicorn.conf.py app:app")
    print("=" * 80)

    try:
        # Import modules on startup
        if import_modules():
            logger.info("✅ All modules imported successfully")
        else:
            logger.warning("⚠️  Some modules failed to import - functionality may be limited")

        # Development configuration
        app.run(
            host=host,
            port=port,
            debug=debug_mode,
            threaded=True,
            use_reloader=debug_mode
        )

    except KeyboardInterrupt:
        print("\n🛑 Server stopped by user")
        logger.info("Server stopped by user")
    except Exception as e:
        print(f"❌ Server failed to start: {e}")
        logger.error(f"Server startup failed: {e}")
        sys.exit(1)

# WSGI entry point
# This is what WSGI servers (gunicorn, uwsgi, etc.) will import
application = create_app()

if __name__ == '__main__':
    run_development_server()
