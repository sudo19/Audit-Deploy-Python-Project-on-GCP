import os
import base64
import re
import pickle
import time
import logging
import schedule
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import json
import argparse
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from contextlib import contextmanager

try:
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from bs4 import BeautifulSoup
except ImportError as e:
    error_msg = f"Missing required dependencies: {e}\nPlease install required packages: pip install -r requirements.txt"
    logging.error(error_msg)
    sys.exit(1)

# Gmail API scope - only reading, no sending
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


@dataclass
class EmailAttachment:
    """Data class for email attachments."""
    original_name: str
    saved_path: str
    size: Optional[int] = None


@dataclass
class SterlingEmail:
    """Data class for Sterling email information."""
    id: str
    subject: str
    sender: str
    sender_email: str
    date: str
    retrieved_at: str
    attachments: List[EmailAttachment]
    email_content_file: str
    email_json_file: str  # New field for individual email JSON
    plain_text_preview: Optional[str] = None


class ConfigurationError(Exception):
    """Custom exception for configuration errors."""
    pass


class AuthenticationError(Exception):
    """Custom exception for authentication errors."""
    pass


class EmailProcessingError(Exception):
    """Custom exception for email processing errors."""
    pass


class Logger:
    """Centralized logging configuration."""

    @staticmethod
    def setup_logger(name: str, log_file: str = "sterling_monitor.log", level: int = logging.INFO) -> logging.Logger:
        """Set up and return a configured logger."""
        logger = logging.getLogger(name)
        logger.setLevel(level)

        # Remove existing handlers to avoid duplicates
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        return logger


class FileManager:
    """Handles file operations and directory management."""

    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)
        self.attachment_dir = self.base_dir / 'attachments'
        self.log_dir = self.base_dir / 'logs'
        self.emails_dir = self.base_dir / 'emails'  # New directory for all emails
        self._setup_directories()

    def _setup_directories(self) -> None:
        """Create necessary directories."""
        for directory in [self.base_dir, self.attachment_dir, self.log_dir, self.emails_dir]:
            directory.mkdir(exist_ok=True)

    def _get_timestamp_string(self) -> str:
        """Get current timestamp as string for filenames."""
        return datetime.now().strftime("%Y%m%d_%H%M%S")

    def _sanitize_timestamp_for_filename(self, timestamp_str: str) -> str:
        """Convert timestamp to filename-safe format."""
        return timestamp_str.replace(' ', '_').replace(':', '').replace('-', '')

    @property
    def last_processed_file(self) -> Path:
        return self.base_dir / 'last_processed_email.txt'

    @property
    def latest_email_file(self) -> Path:
        """Get latest email file with timestamp."""
        timestamp = self._get_timestamp_string()
        return self.base_dir / f'latest_sterling_email_{timestamp}.json'

    def get_timestamped_email_file(self, email_id: str, retrieved_at: str = None) -> Path:
        """Get timestamped email file path."""
        if retrieved_at:
            # Convert retrieved_at to filename-safe format
            timestamp = self._sanitize_timestamp_for_filename(retrieved_at)
        else:
            timestamp = self._get_timestamp_string()
        return self.emails_dir / f'sterling_email_{email_id}_{timestamp}.json'

    def get_all_emails_summary_file(self) -> Path:
        """Get summary file for all emails with timestamp."""
        timestamp = self._get_timestamp_string()
        return self.base_dir / f'all_sterling_emails_{timestamp}.json'

    def get_timestamped_content_file(self, email_id: str, retrieved_at: str = None) -> Path:
        """Get timestamped email content file path."""
        if retrieved_at:
            timestamp = self._sanitize_timestamp_for_filename(retrieved_at)
        else:
            timestamp = self._get_timestamp_string()
        return self.base_dir / f'sterling_content_{email_id}_{timestamp}.txt'

    @property
    def summary_file(self) -> Path:
        """Legacy summary file - kept for backward compatibility."""
        return self.base_dir / 'sterling_emails_summary.json'

    def save_json(self, data: Any, file_path: Path) -> None:
        """Save data to JSON file."""
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=str)

    def load_json(self, file_path: Path) -> Optional[Dict]:
        """Load data from JSON file."""
        if not file_path.exists():
            return None
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return None

    def read_text_file(self, file_path: Path) -> Optional[str]:
        """Read text from file."""
        if not file_path.exists():
            return None
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read().strip()
        except Exception:
            return None

    def write_text_file(self, file_path: Path, content: str) -> None:
        """Write text to file."""
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)

    def get_all_processed_emails(self) -> List[Dict]:
        """Get list of all processed emails from the emails directory."""
        all_emails = []
        if not self.emails_dir.exists():
            return all_emails

        for json_file in self.emails_dir.glob('sterling_email_*.json'):
            email_data = self.load_json(json_file)
            if email_data:
                all_emails.append(email_data)

        # Sort by retrieved_at timestamp (newest first)
        all_emails.sort(key=lambda x: x.get('retrieved_at', ''), reverse=True)
        return all_emails


class HTMLProcessor:
    """Handles HTML to text conversion."""

    @staticmethod
    def html_to_plain_text(html_content: str) -> str:
        """Convert HTML content to plain text."""
        if not html_content:
            return ""

        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.extract()

            # Get text
            text = soup.get_text(separator='\n')

            # Clean up whitespace
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = '\n'.join(chunk for chunk in chunks if chunk)

            return text
        except Exception:
            # Fallback to regex-based HTML tag removal
            return re.sub(r'<[^>]+>', ' ', html_content).strip()


class GmailAuthenticator:
    """Handles Gmail API authentication."""

    def __init__(self, credentials_file: str = 'credentials.json', token_file: str = 'token.pickle'):
        self.credentials_file = credentials_file
        self.token_file = Path(token_file)
        self.logger = Logger.setup_logger(self.__class__.__name__)

    def get_credentials(self) -> Credentials:
        """Get or create Gmail API credentials."""
        creds = self._load_existing_credentials()

        if not creds or not creds.valid:
            creds = self._refresh_or_create_credentials(creds)
            self._save_credentials(creds)

        return creds

    def _load_existing_credentials(self) -> Optional[Credentials]:
        """Load credentials from token file."""
        if not self.token_file.exists():
            return None

        try:
            with open(self.token_file, 'rb') as token:
                return pickle.load(token)
        except Exception as e:
            self.logger.warning(f"Failed to load credentials: {e}")
            return None

    def _refresh_or_create_credentials(self, creds: Optional[Credentials]) -> Credentials:
        """Refresh expired credentials or create new ones."""
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                self.logger.debug("Refreshed expired credentials")
                return creds
            except Exception as e:
                self.logger.error(f"Failed to refresh credentials: {e}")

        return self._create_new_credentials()

    def _create_new_credentials(self) -> Credentials:
        """Create new credentials via OAuth flow."""
        if not Path(self.credentials_file).exists():
            raise AuthenticationError(f"Credentials file '{self.credentials_file}' not found.")

        try:
            self.logger.info("Getting new credentials. Browser will open for authorization.")
            flow = InstalledAppFlow.from_client_secrets_file(self.credentials_file, SCOPES)
            creds = flow.run_local_server(port=0)
            self.logger.info("Successfully obtained new credentials")
            return creds
        except Exception as e:
            raise AuthenticationError(f"Failed to get credentials: {e}")

    def _save_credentials(self, creds: Credentials) -> None:
        """Save credentials to token file."""
        try:
            with open(self.token_file, 'wb') as token:
                pickle.dump(creds, token)
            self.logger.debug(f"Saved credentials to {self.token_file}")
        except Exception as e:
            self.logger.warning(f"Failed to save credentials: {e}")


class EmailContentExtractor:
    """Handles email content extraction."""

    def __init__(self):
        self.html_processor = HTMLProcessor()
        self.logger = Logger.setup_logger(self.__class__.__name__)

    def extract_content(self, message_detail: Dict) -> Dict[str, str]:
        """Extract email content from message detail."""
        payload = message_detail['payload']
        mime_type = payload.get('mimeType', '')

        plain_text = ""
        html_content = ""

        if mime_type == 'text/plain':
            plain_text = self._decode_email_body(payload)
        elif mime_type == 'text/html':
            html_content = self._decode_email_body(payload)
            plain_text = self.html_processor.html_to_plain_text(html_content)
        elif 'multipart' in mime_type:
            plain_text, html_content = self._extract_multipart_content(payload)

        # Convert HTML to plain text if needed
        if html_content and not plain_text:
            plain_text = self.html_processor.html_to_plain_text(html_content)

        return {'plain': plain_text, 'html': html_content}

    def _decode_email_body(self, message_part: Dict) -> str:
        """Decode email body content from a message part."""
        if message_part.get('body', {}).get('data'):
            try:
                data = message_part['body']['data']
                return base64.urlsafe_b64decode(data).decode('utf-8', errors='replace')
            except Exception as e:
                self.logger.warning(f"Error decoding email body: {e}")
                return ""

        # Handle nested parts
        if message_part.get('parts'):
            text_parts = [self._decode_email_body(part) for part in message_part['parts']]
            return '\n'.join(part for part in text_parts if part)

        return ""

    def _extract_multipart_content(self, payload: Dict) -> tuple[str, str]:
        """Extract content from multipart messages."""
        plain_text = ""
        html_content = ""

        for part in payload.get('parts', []):
            part_mime = part.get('mimeType', '')
            if part_mime == 'text/plain':
                plain_text += self._decode_email_body(part)
            elif part_mime == 'text/html':
                html_content += self._decode_email_body(part)
            elif 'multipart' in part_mime and part.get('parts'):
                # Handle nested multipart
                nested_plain, nested_html = self._extract_multipart_content(part)
                plain_text += nested_plain
                html_content += nested_html

        return plain_text, html_content


class AttachmentProcessor:
    """Handles email attachment processing."""

    def __init__(self, attachment_dir: Path):
        self.attachment_dir = attachment_dir
        self.logger = Logger.setup_logger(self.__class__.__name__)

    def process_attachments(self, service, message_id: str, message_detail: Dict, retrieved_at: str) -> List[EmailAttachment]:
        """Process and download attachments from an email."""
        attachments = []

        if 'parts' not in message_detail['payload']:
            return attachments

        for part in message_detail['payload']['parts']:
            if 'filename' in part and part['filename']:
                attachment = self._download_attachment(service, message_id, part, retrieved_at)
                if attachment:
                    attachments.append(attachment)

        return attachments

    def _download_attachment(self, service, message_id: str, part: Dict, retrieved_at: str) -> Optional[EmailAttachment]:
        """Download a single attachment."""
        filename = part['filename']
        attachment_id = part['body'].get('attachmentId')

        if not attachment_id:
            return None

        try:
            # Get the attachment data
            attachment = service.users().messages().attachments().get(
                userId='me', messageId=message_id, id=attachment_id
            ).execute()

            # Decode and save with timestamp
            data = base64.urlsafe_b64decode(attachment['data'])

            # Create timestamped filename
            timestamp = retrieved_at.replace(' ', '_').replace(':', '').replace('-', '')
            file_extension = Path(filename).suffix
            file_stem = Path(filename).stem
            safe_filename = f"sterling_{message_id}_{timestamp}_{file_stem}{file_extension}"
            file_path = self.attachment_dir / safe_filename

            with open(file_path, 'wb') as f:
                f.write(data)

            self.logger.info(f"Downloaded attachment: {filename} -> {file_path}")

            return EmailAttachment(
                original_name=filename,
                saved_path=str(file_path),
                size=len(data)
            )
        except Exception as e:
            self.logger.error(f"Failed to process attachment {filename}: {e}")
            return None


class EmailParser:
    """Parses email headers and metadata."""

    @staticmethod
    def extract_headers(message_detail: Dict) -> Dict[str, str]:
        """Extract relevant headers from email."""
        headers = message_detail['payload']['headers']
        header_dict = {h['name'].lower(): h['value'] for h in headers}

        subject = header_dict.get('subject', 'No subject')
        sender = header_dict.get('from', 'Unknown')
        date = header_dict.get('date', 'Unknown')

        # Extract sender email
        sender_email_match = re.search(r'<(.+?)>', sender)
        sender_email = sender_email_match.group(1) if sender_email_match else sender

        return {
            'subject': subject,
            'sender': sender,
            'sender_email': sender_email,
            'date': date
        }


class SterlingEmailMonitor:
    """Main class for monitoring Sterling Ornaments emails."""

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.logger = Logger.setup_logger(self.__class__.__name__)

        # Initialize components
        self.file_manager = FileManager(self.config.get('base_dir', 'sterling_emails'))
        self.authenticator = GmailAuthenticator(
            self.config.get('credentials_file', 'credentials.json'),
            self.config.get('token_file', 'token.pickle')
        )
        self.content_extractor = EmailContentExtractor()
        self.attachment_processor = AttachmentProcessor(self.file_manager.attachment_dir)

        self._gmail_service = None
        self._setup_signal_handlers()

        self.logger.info(f"Sterling Email Monitor initialized. Base directory: {self.file_manager.base_dir}")

    def _setup_signal_handlers(self) -> None:
        """Set up signal handlers for graceful shutdown."""
        def signal_handler(sig, frame):
            self.logger.info("Received termination signal. Shutting down...")
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    @property
    def gmail_service(self):
        """Get Gmail service with lazy initialization."""
        if self._gmail_service is None:
            try:
                creds = self.authenticator.get_credentials()
                self._gmail_service = build('gmail', 'v1', credentials=creds)
                self.logger.info("Successfully authenticated with Gmail API")
            except Exception as e:
                raise AuthenticationError(f"Failed to build Gmail service: {e}")

        return self._gmail_service

    def search_sterling_emails(self, most_recent_only: bool = True) -> List[SterlingEmail]:
        """Search for Sterling emails with attachments."""
        self.logger.info("Checking for Sterling emails...")

        # More flexible search query to catch variations
        search_query = "subject:(STERLING outstanding order) has:attachment"
        max_results = 1 if most_recent_only else 10

        try:
            results = self.gmail_service.users().messages().list(
                userId='me', q=search_query, maxResults=max_results
            ).execute()

            messages = results.get('messages', [])

            if not messages:
                self.logger.info("No Sterling emails with attachments found.")
                return []

            # Check if we've already processed the most recent email
            if most_recent_only and self._is_already_processed(messages[0]['id']):
                self.logger.info("Most recent email already processed. No new emails to process.")
                return []

            if most_recent_only:
                messages = [messages[0]]
                self.logger.info("Found a new Sterling email with attachments. Processing...")
            else:
                self.logger.info(f"Found {len(messages)} Sterling emails with attachments. Processing...")

            return self._process_messages(messages)

        except Exception as e:
            raise EmailProcessingError(f"Error processing emails: {e}")

    def _is_already_processed(self, message_id: str) -> bool:
        """Check if a message has already been processed."""
        last_processed_id = self.file_manager.read_text_file(self.file_manager.last_processed_file)
        return last_processed_id == message_id

    def _process_messages(self, messages: List[Dict]) -> List[SterlingEmail]:
        """Process a list of email messages."""
        sterling_emails = []
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for message in messages:
            try:
                sterling_email = self._process_single_message(message, current_time)
                if sterling_email:
                    sterling_emails.append(sterling_email)
            except Exception as e:
                self.logger.error(f"Failed to process message {message['id']}: {e}")
                continue

        if sterling_emails:
            self._save_results(sterling_emails, len(messages) == 1)

        return sterling_emails

    def _process_single_message(self, message: Dict, current_time: str) -> Optional[SterlingEmail]:
        """Process a single email message."""
        msg_id = message['id']

        try:
            message_detail = self.gmail_service.users().messages().get(userId='me', id=msg_id).execute()
        except HttpError as e:
            self.logger.error(f"Failed to fetch email details for ID {msg_id}: {e}")
            return None

        # Extract headers
        headers = EmailParser.extract_headers(message_detail)

        self.logger.info(f"Processing email ID: {msg_id}")
        self.logger.info(f"From: {headers['sender']}")
        self.logger.info(f"Date: {headers['date']}")
        self.logger.info(f"Subject: {headers['subject']}")

        # Extract content
        email_content = self.content_extractor.extract_content(message_detail)

        # Save email content with timestamp
        email_content_path = self._save_email_content(msg_id, headers, email_content['plain'], current_time)

        # Process attachments with timestamp
        attachments = self.attachment_processor.process_attachments(
            self.gmail_service, msg_id, message_detail, current_time
        )

        if not attachments:
            self.logger.warning(f"Email with ID: {msg_id} doesn't have valid attachments, skipping.")
            return None

        # Create preview (first 200 characters)
        preview = email_content['plain'][:200] + "..." if len(email_content['plain']) > 200 else email_content['plain']

        # Get individual email JSON file path
        email_json_path = self.file_manager.get_timestamped_email_file(msg_id, current_time)

        sterling_email = SterlingEmail(
            id=msg_id,
            subject=headers['subject'],
            sender=headers['sender'],
            sender_email=headers['sender_email'],
            date=headers['date'],
            retrieved_at=current_time,
            attachments=attachments,
            email_content_file=str(email_content_path),
            email_json_file=str(email_json_path),
            plain_text_preview=preview
        )

        # Save individual email JSON
        self.file_manager.save_json(asdict(sterling_email), email_json_path)
        self.logger.info(f"Saved individual email JSON to: {email_json_path}")

        return sterling_email

    def _save_email_content(self, msg_id: str, headers: Dict, content: str, current_time: str) -> Path:
        """Save email content to file with timestamp."""
        email_path = self.file_manager.get_timestamped_content_file(msg_id, current_time)

        email_content = f"""Subject: {headers['subject']}
From: {headers['sender']}
Date: {headers['date']}
Email ID: {msg_id}
Retrieved at: {current_time}
{'-' * 50}

{content}"""

        self.file_manager.write_text_file(email_path, email_content)
        self.logger.info(f"Saved email content to: {email_path}")

        return email_path

    def _save_results(self, sterling_emails: List[SterlingEmail], is_single: bool) -> None:
        """Save processing results to files."""
        try:
            if is_single:
                # Save single email details with timestamp
                email_dict = asdict(sterling_emails[0])
                latest_file = self.file_manager.latest_email_file
                self.file_manager.save_json(email_dict, latest_file)
                self.logger.info(f"Saved latest email details to: {latest_file}")
            else:
                # Save all emails summary with timestamp
                emails_dict = [asdict(email) for email in sterling_emails]
                summary_file = self.file_manager.get_all_emails_summary_file()
                self.file_manager.save_json(emails_dict, summary_file)
                self.logger.info(f"Saved all emails summary to: {summary_file}")

                # Also update legacy summary file
                self.file_manager.save_json(emails_dict, self.file_manager.summary_file)

            # Update last processed email ID
            if sterling_emails:
                self.file_manager.write_text_file(
                    self.file_manager.last_processed_file,
                    sterling_emails[0].id
                )

            # Log attachment summary
            total_attachments = sum(len(email.attachments) for email in sterling_emails)
            self.logger.info(f"Found {total_attachments} attachments across {len(sterling_emails)} email(s)")

        except Exception as e:
            self.logger.error(f"Failed to save results: {e}")

    def get_all_processed_emails_summary(self) -> Dict:
        """Get summary of all processed emails."""
        all_emails = self.file_manager.get_all_processed_emails()

        summary = {
            'total_emails_processed': len(all_emails),
            'last_updated': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'emails': all_emails
        }

        return summary

    def run_scheduled_check(self) -> None:
        """Run a single scheduled check."""
        self.logger.info("=" * 50)
        self.logger.info(f"SCHEDULED CHECK: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("=" * 50)

        try:
            sterling_emails = self.search_sterling_emails(most_recent_only=True)

            if sterling_emails:
                email = sterling_emails[0]
                self.logger.info("New Sterling email found and processed:")
                self.logger.info(f"Subject: {email.subject}")
                self.logger.info(f"From: {email.sender}")
                self.logger.info(f"Date: {email.date}")
                self.logger.info(f"Retrieved at: {email.retrieved_at}")
                self.logger.info(f"Email content saved to: {email.email_content_file}")
                self.logger.info(f"Email JSON saved to: {email.email_json_file}")

                if email.attachments:
                    self.logger.info(f"Attachments ({len(email.attachments)}):")
                    for i, attachment in enumerate(email.attachments, 1):
                        # Return full absolute path for deployment clarity
                        full_path = Path(attachment.saved_path).resolve()
                        self.logger.info(f"{i}. {attachment.original_name}")
                        self.logger.info(f"   Full path: {full_path}")
                        if attachment.size:
                            self.logger.info(f"   Size: {attachment.size:,} bytes")
                else:
                    self.logger.info("No attachments found in this email")

                # Display file paths summary
                self.logger.info("\nFile Paths Summary:")
                self.logger.info(f"Base directory: {self.file_manager.base_dir}")
                self.logger.info(f"Attachments directory: {self.file_manager.attachment_dir}")
                self.logger.info(f"Emails directory: {self.file_manager.emails_dir}")
                self.logger.info(f"Email content file: {email.email_content_file}")
                self.logger.info(f"Email JSON file: {email.email_json_file}")
                self.logger.info(f"Last processed ID file: {self.file_manager.last_processed_file}")
            else:
                self.logger.info("No new Sterling emails found during this check.")

            self.logger.info("Waiting for next scheduled check...")

        except Exception as e:
            self.logger.error(f"Error during scheduled check: {e}")

    def start_monitoring(self, interval_hours: float = 1.0) -> None:
        """Start monitoring for Sterling Ornaments emails at specified interval."""
        self.logger.info(f"Starting Sterling Email Monitor to run every {interval_hours} hour(s)")
        self.logger.info(f"Files will be saved to: {self.file_manager.base_dir.resolve()}")

        # Schedule the job
        schedule.every(interval_hours).hours.do(self.run_scheduled_check)

        # Run initial check
        self.logger.info("Running initial check...")
        self.run_scheduled_check()

        # Keep the script running
        try:
            self.logger.info("Monitoring started. Press Ctrl+C to stop.")
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check for pending jobs every minute
        except KeyboardInterrupt:
            self.logger.info("Monitoring stopped by user.")
        except Exception as e:
            self.logger.critical(f"Unexpected error in monitoring loop: {e}")
            sys.exit(1)


def load_config(config_path: str) -> Dict:
    """Load configuration from file."""
    config_file = Path(config_path)
    if not config_file.exists():
        return {}

    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.warning(f"Failed to load configuration file: {e}")
        return {}


def main(input_file=None, interval=1.0, config_path='config.json', single_run=True, verbose=False):
    """
    Main function for Sterling Ornaments Email Monitor that can be imported.

    Args:
        input_file: Not used in this email monitor (kept for compatibility)
        interval: Check interval in hours (default: 1.0)
        config_path: Path to configuration file (default: 'config.json')
        single_run: Run once and exit (default: True for import usage)
        verbose: Enable verbose logging (default: False)

    Returns:
        dict: Result dictionary with status, message, and output_file
    """
    import logging
    from pathlib import Path

    try:
        # Load configuration
        config = load_config(config_path)

        # Set up logging level
        log_level = logging.DEBUG if verbose else logging.INFO

        # Create the monitor
        monitor = SterlingEmailMonitor(config)

        if single_run:
            monitor.logger.info("Running email check...")
            monitor.run_scheduled_check()

            # Check if we got any results
            latest_email_data = None

            # Try to find the most recent email JSON file
            if monitor.file_manager.emails_dir.exists():
                json_files = list(monitor.file_manager.emails_dir.glob('sterling_email_*.json'))
                if json_files:
                    # Sort by modification time to get the most recent
                    latest_json_file = max(json_files, key=lambda x: x.stat().st_mtime)
                    latest_email_data = monitor.file_manager.load_json(latest_json_file)

            if latest_email_data:
                # Find XLSX file in attachments
                xlsx_path = None
                all_attachments = []

                if latest_email_data.get('attachments'):
                    for attachment in latest_email_data['attachments']:
                        full_path = Path(attachment['saved_path']).resolve()
                        all_attachments.append(str(full_path))

                        # Look for XLSX file
                        if attachment['original_name'].lower().endswith('.xlsx') and xlsx_path is None:
                            xlsx_path = str(full_path)

                # Get summary of all processed emails
                all_emails_summary = monitor.get_all_processed_emails_summary()

                if xlsx_path:
                    return {
                        'status': 'success',
                        'message': f"Successfully processed Sterling email (Retrieved at: {latest_email_data.get('retrieved_at')}). Found XLSX attachment: {Path(xlsx_path).name}",
                        'output_file': xlsx_path,
                        'email_data': latest_email_data,
                        'all_attachments': all_attachments,
                        'all_emails_summary': all_emails_summary
                    }
                elif all_attachments:
                    return {
                        'status': 'success',
                        'message': f"Successfully processed Sterling email (Retrieved at: {latest_email_data.get('retrieved_at')}). Found {len(all_attachments)} attachment(s), but no XLSX file.",
                        'output_file': all_attachments[0],  # Return first attachment
                        'email_data': latest_email_data,
                        'all_attachments': all_attachments,
                        'all_emails_summary': all_emails_summary
                    }
                else:
                    return {
                        'status': 'success',
                        'message': f"Successfully processed Sterling email (Retrieved at: {latest_email_data.get('retrieved_at')}), but no attachments found.",
                        'output_file': latest_email_data.get('email_content_file'),
                        'email_data': latest_email_data,
                        'all_attachments': [],
                        'all_emails_summary': all_emails_summary
                    }
            else:
                # Get summary even if no new emails
                all_emails_summary = monitor.get_all_processed_emails_summary()
                return {
                    'status': 'success',
                    'message': "No new Sterling emails found.",
                    'output_file': None,
                    'email_data': None,
                    'all_attachments': [],
                    'all_emails_summary': all_emails_summary
                }
        else:
            # For continuous monitoring (not typical for import usage)
            monitor.start_monitoring(interval_hours=interval)
            return {
                'status': 'success',
                'message': "Monitoring started (continuous mode)",
                'output_file': None,
                'email_data': None,
                'all_attachments': [],
                'all_emails_summary': {}
            }

    except (ConfigurationError, AuthenticationError, EmailProcessingError) as e:
        return {
            'status': 'error',
            'message': f"Configuration/Authentication Error: {str(e)}",
            'output_file': None,
            'email_data': None,
            'all_attachments': [],
            'all_emails_summary': {}
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': f"Unexpected error: {str(e)}",
            'output_file': None,
            'email_data': None,
            'all_attachments': [],
            'all_emails_summary': {}
        }


# Example usage
if __name__ == "__main__":
    result = main(single_run=True, verbose=True)

    if result['status'] == 'success':
        print(f"Processing completed: {result['message']}")
        if result['output_file']:
            print(f"Output file: {result['output_file']}")

        # Print summary of all emails
        if result['all_emails_summary']:
            summary = result['all_emails_summary']
            print(f"\nAll Emails Summary:")
            print(f"Total emails processed: {summary['total_emails_processed']}")
            print(f"Last updated: {summary['last_updated']}")

            if summary['emails']:
                print("\nRecent emails:")
                for i, email in enumerate(summary['emails'][:3], 1):  # Show last 3 emails
                    print(f"{i}. Subject: {email.get('subject', 'N/A')}")
                    print(f"   Retrieved at: {email.get('retrieved_at', 'N/A')}")
                    print(f"   Attachments: {len(email.get('attachments', []))}")

        # You can also access:
        # result['email_data'] - the full latest email data
        # result['all_attachments'] - list of all attachment paths from latest email
        # result['all_emails_summary'] - summary of all processed emails
    else:
        print(f"Error: {result['message']}")


# Additional utility functions for managing emails

def get_all_sterling_emails(base_dir='sterling_emails'):
    """Utility function to get all processed Sterling emails."""
    file_manager = FileManager(Path(base_dir))
    return file_manager.get_all_processed_emails()


def cleanup_old_emails(base_dir='sterling_emails', days_to_keep=30):
    """Utility function to cleanup old email files."""
    from datetime import datetime, timedelta
    import os

    file_manager = FileManager(Path(base_dir))
    cutoff_date = datetime.now() - timedelta(days=days_to_keep)

    deleted_count = 0

    # Clean up old email JSON files
    if file_manager.emails_dir.exists():
        for json_file in file_manager.emails_dir.glob('sterling_email_*.json'):
            if json_file.stat().st_mtime < cutoff_date.timestamp():
                json_file.unlink()
                deleted_count += 1

    # Clean up old attachments
    if file_manager.attachment_dir.exists():
        for attachment_file in file_manager.attachment_dir.glob('sterling_*'):
            if attachment_file.stat().st_mtime < cutoff_date.timestamp():
                attachment_file.unlink()
                deleted_count += 1

    # Clean up old content files
    for content_file in file_manager.base_dir.glob('sterling_content_*.txt'):
        if content_file.stat().st_mtime < cutoff_date.timestamp():
            content_file.unlink()
            deleted_count += 1

    return deleted_count


def search_emails_by_date_range(base_dir='sterling_emails', start_date=None, end_date=None):
    """Search for emails within a specific date range."""
    from datetime import datetime

    all_emails = get_all_sterling_emails(base_dir)
    filtered_emails = []

    for email in all_emails:
        try:
            retrieved_at = datetime.strptime(email.get('retrieved_at', ''), '%Y-%m-%d %H:%M:%S')

            if start_date and retrieved_at < start_date:
                continue
            if end_date and retrieved_at > end_date:
                continue

            filtered_emails.append(email)
        except ValueError:
            # Skip emails with invalid date format
            continue

    return filtered_emails

