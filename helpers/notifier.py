"""
Email notification utilities
"""

import os
import logging


def send_alert_email(
    sync_id: str,
    file_name: str,
    error_message: str,
    validation_errors: list = None,
    is_warning: bool = False
):
    """
    Send alert email on sync failure or warning.

    Note: This is a placeholder implementation.
    In production, integrate with SendGrid, Azure Communication Services,
    or other email service.
    """
    alert_email = os.getenv('ALERT_EMAIL', 'ops-team@company.com')

    subject = f"{'Warning' if is_warning else 'Error'}: ZTDWR Sync {sync_id}"

    body = f"""
ZTDWR Data Sync {'Warning' if is_warning else 'Failure'}

Sync ID: {sync_id}
File: {file_name}
Status: {'PARTIAL_SUCCESS' if is_warning else 'FAILED'}
Error: {error_message}
"""

    if validation_errors:
        body += f"\n\nValidation Errors: {len(validation_errors)}\n"
        # Include first 5 errors as sample
        for err in validation_errors[:5]:
            body += f"- Row {err.get('row', 'N/A')}: {err.get('errors', err.get('message', 'Unknown'))}\n"

        if len(validation_errors) > 5:
            body += f"... and {len(validation_errors) - 5} more errors\n"

    body += f"\n\nPlease check Azure Function logs for details."

    # Log the email (actual sending would go here)
    logging.warning(f"Alert email would be sent to {alert_email}")
    logging.warning(f"Subject: {subject}")
    logging.warning(f"Body:\n{body}")

    # TODO: Implement actual email sending
    # Example with SendGrid:
    # from sendgrid import SendGridAPIClient
    # from sendgrid.helpers.mail import Mail
    #
    # message = Mail(
    #     from_email='noreply@company.com',
    #     to_emails=alert_email,
    #     subject=subject,
    #     plain_text_content=body
    # )
    # sg = SendGridAPIClient(os.getenv('SENDGRID_API_KEY'))
    # response = sg.send(message)

    logging.info(f"Alert notification prepared for {alert_email}")
