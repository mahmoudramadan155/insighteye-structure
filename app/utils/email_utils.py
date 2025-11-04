# app/utils/email_utils.py
"""Email utilities for sending notifications and alerts."""

import logging
import asyncio
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from app.config.settings import config

logger = logging.getLogger(__name__)


async def send_email(
    recipient_email: str, 
    subject: str, 
    body: str, 
    html_body: Optional[str] = None
) -> bool:
    """Send email asynchronously."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, 
        _send_email_sync, 
        recipient_email, 
        subject, 
        body, 
        html_body
    )


def _send_email_sync(
    recipient_email: str, 
    subject: str, 
    body: str, 
    html_body: Optional[str] = None
) -> bool:
    """Synchronous helper for sending email."""
    app_sender_email = config.get("otp_sender_email", config.get("sender_email"))
    app_sender_password = config.get("smtp_password", config.get("sender_password"))
    smtp_server_host = config.get("smtp_server")
    smtp_server_port = int(config.get("smtp_port", 587))

    if not all([app_sender_email, app_sender_password, smtp_server_host, smtp_server_port]):
        logger.error("SMTP server, port, or credentials not fully configured for send_email.")
        return False

    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = app_sender_email
    message["To"] = recipient_email
    
    message.attach(MIMEText(body, "plain", _charset="utf-8"))
    if html_body:
        message.attach(MIMEText(html_body, "html", _charset="utf-8"))

    context = ssl.create_default_context()
    try:
        with smtplib.SMTP(smtp_server_host, smtp_server_port, timeout=config.get("smtp_timeout", 30)) as server:
            server.ehlo_or_helo_if_needed()
            if server.has_extn('STARTTLS'):
                server.starttls(context=context)
                server.ehlo_or_helo_if_needed()
            server.login(app_sender_email, app_sender_password)
            server.sendmail(app_sender_email, recipient_email, message.as_string())
        logger.info(f"Email sent successfully to {recipient_email} with subject '{subject}'")
        return True
    except smtplib.SMTPAuthenticationError:
        logger.error(f"SMTP Authentication Error for {app_sender_email}. Check credentials.", exc_info=True)
        return False
    except smtplib.SMTPRecipientsRefused:
        logger.error(f"Recipient refused for email to {recipient_email}.", exc_info=True)
        return False
    except smtplib.SMTPException as e_smtp:
        logger.error(f"SMTP Error sending email to {recipient_email}: {e_smtp}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"General error in _send_email_sync for {recipient_email}: {e}", exc_info=True)
        return False


async def send_email_from_client_to_admin(
    admin_recipient_email: str, 
    subject: str, 
    body: str, 
    reply_to_email: Optional[str] = None
) -> bool:
    """Send notification email to admin from client."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, 
        _send_email_from_client_to_admin_sync, 
        admin_recipient_email, 
        subject, 
        body, 
        reply_to_email
    )


def _send_email_from_client_to_admin_sync(
    admin_recipient_email: str, 
    subject: str, 
    body: str, 
    reply_to_email: Optional[str] = None
) -> bool:
    """Synchronous helper for sending admin notification email."""
    app_sender_email = config.get("otp_sender_email", config.get("sender_email"))
    app_sender_password = config.get("smtp_password", config.get("sender_password"))
    smtp_server_host = config.get("smtp_server")
    smtp_server_port = int(config.get("smtp_port", 587))

    if not all([app_sender_email, app_sender_password, smtp_server_host, smtp_server_port]):
        logger.error("SMTP config incomplete for send_email_from_client_to_admin.")
        return False

    message = MIMEMultipart()
    message["From"] = f"InsightEye System <{app_sender_email}>" 
    message["To"] = admin_recipient_email
    message["Subject"] = subject
    if reply_to_email:
        message.add_header('Reply-To', reply_to_email)
    
    message.attach(MIMEText(body, "plain", _charset="utf-8"))

    context = ssl.create_default_context()
    try:
        with smtplib.SMTP(smtp_server_host, smtp_server_port, timeout=config.get("smtp_timeout", 30)) as server:
            server.ehlo_or_helo_if_needed()
            if server.has_extn('STARTTLS'):
                server.starttls(context=context)
                server.ehlo_or_helo_if_needed()
            server.login(app_sender_email, app_sender_password)
            server.sendmail(app_sender_email, admin_recipient_email, message.as_string())
        logger.info(f"Admin notification email sent to {admin_recipient_email} regarding '{subject}' from {reply_to_email or 'system'}")
        return True
    except smtplib.SMTPAuthenticationError:
        logger.error(f"SMTP Authentication Error for {app_sender_email} (admin notification). Check credentials.", exc_info=True)
        return False
    except smtplib.SMTPRecipientsRefused:
        logger.error(f"Recipient refused for admin notification email to {admin_recipient_email}.", exc_info=True)
        return False
    except smtplib.SMTPException as e_smtp:
        logger.error(f"SMTP Error sending admin notification to {admin_recipient_email}: {e_smtp}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"General error in _send_email_from_client_to_admin_sync for {admin_recipient_email}: {e}", exc_info=True)
        return False


async def send_fire_alert_email(
    user_email: str, 
    camera_name: str, 
    fire_status: str, 
    location_info: Optional[Dict[str, Any]] = None
) -> bool:
    """Send fire/smoke alert email to user."""
    try:
        logger.info(f"Preparing to send fire alert email to {user_email} for {fire_status} detected at {camera_name}")
        
        location_text = ""
        location_parts = []
        
        if location_info:
            if location_info.get('location'):
                location_parts.append(f"Location: {location_info['location']}")
            if location_info.get('building'):
                location_parts.append(f"Building: {location_info['building']}")
            if location_info.get('area'):
                location_parts.append(f"Area: {location_info['area']}")
            if location_info.get('zone'):
                location_parts.append(f"Zone: {location_info['zone']}")
            if location_info.get('floor_level'):
                location_parts.append(f"Floor: {location_info['floor_level']}")
            
            if location_parts:
                location_text = f"\nLocation Details:\n" + "\n".join(f"  • {part}" for part in location_parts)
        
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        alert_type = "FIRE" if fire_status == "fire" else "SMOKE"
        subject = f"🚨 URGENT: {alert_type} DETECTED - {camera_name}"
        
        body = f"""URGENT ALERT: {alert_type} DETECTED

Camera: {camera_name}
Status: {fire_status.upper()}
Time: {timestamp}{location_text}

This is an automated alert from your InsightEye surveillance system. 
Please verify the situation immediately and take appropriate action.

If this is a false alarm, please check your camera positioning and detection settings.

---
InsightEye Surveillance System
This alert will not be sent again for the next 10 minutes to prevent spam.
"""

        html_body = f"""
<html>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
    <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
        <div style="background-color: #dc3545; color: white; padding: 15px; border-radius: 5px; text-align: center; margin-bottom: 20px;">
            <h1 style="margin: 0; font-size: 24px;">🚨 URGENT ALERT</h1>
            <h2 style="margin: 5px 0 0 0; font-size: 20px;">{alert_type} DETECTED</h2>
        </div>
        
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 5px; margin-bottom: 20px;">
            <h3 style="color: #dc3545; margin-top: 0;">Alert Details:</h3>
            <ul style="list-style: none; padding: 0;">
                <li style="padding: 5px 0;"><strong>Camera:</strong> {camera_name}</li>
                <li style="padding: 5px 0;"><strong>Status:</strong> <span style="color: #dc3545; font-weight: bold;">{fire_status.upper()}</span></li>
                <li style="padding: 5px 0;"><strong>Time:</strong> {timestamp}</li>
            </ul>
            {f'<h4>Location Details:</h4><ul style="list-style: none; padding-left: 20px;">' + ''.join(f'<li style="padding: 2px 0;">• {part}</li>' for part in location_parts) + '</ul>' if location_parts else ''}
        </div>
        
        <div style="background-color: #fff3cd; border: 1px solid #ffeaa7; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
            <h4 style="color: #856404; margin-top: 0;">⚠️ Immediate Action Required</h4>
            <p style="margin-bottom: 0; color: #856404;">Please verify the situation immediately and take appropriate safety measures.</p>
        </div>
        
        <div style="font-size: 12px; color: #6c757d; border-top: 1px solid #dee2e6; padding-top: 15px;">
            <p>This is an automated alert from your InsightEye surveillance system.</p>
            <p>If this is a false alarm, please check your camera positioning and detection settings.</p>
            <p><strong>Note:</strong> This alert will not be sent again for the next 10 minutes to prevent spam.</p>
        </div>
    </div>
</body>
</html>
"""

        success = await send_email(user_email, subject, body, html_body=html_body)
        if success:
            logger.info(f"Fire alert email sent successfully to {user_email} for {fire_status} at {camera_name}")
        else:
            logger.error(f"Failed to send fire alert email to {user_email} for {fire_status} at {camera_name}")
        return success
        
    except Exception as e:
        logger.error(f"Error sending fire alert email to {user_email}: {e}", exc_info=True)
        return False


async def send_people_count_alert_email(
    user_email: str, 
    camera_name: str, 
    person_count: int, 
    threshold_settings: Dict[str, Any],
    location_info: Optional[Dict[str, Any]] = None
) -> bool:
    """Send people count threshold alert email to user."""
    try:
        logger.info(f"Preparing to send people count alert email to {user_email} for {person_count} people at {camera_name}")
        
        location_text = ""
        location_parts = []
        
        if location_info:
            if location_info.get('location'):
                location_parts.append(f"Location: {location_info['location']}")
            if location_info.get('building'):
                location_parts.append(f"Building: {location_info['building']}")
            if location_info.get('area'):
                location_parts.append(f"Area: {location_info['area']}")
            if location_info.get('zone'):
                location_parts.append(f"Zone: {location_info['zone']}")
            if location_info.get('floor_level'):
                location_parts.append(f"Floor: {location_info['floor_level']}")
            
            if location_parts:
                location_text = f"\nLocation Details:\n" + "\n".join(f"  • {part}" for part in location_parts)
        
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        
        greater_than = threshold_settings.get("greater_than")
        less_than = threshold_settings.get("less_than")
        
        if greater_than is not None and person_count > greater_than:
            alert_type = "HIGH OCCUPANCY"
            threshold_info = f"Detected: {person_count} people (Threshold: >{greater_than})"
            severity_color = "#dc3545"
        elif less_than is not None and person_count < less_than:
            alert_type = "LOW OCCUPANCY"
            threshold_info = f"Detected: {person_count} people (Threshold: <{less_than})"
            severity_color = "#fd7e14"
        else:
            alert_type = "OCCUPANCY"
            threshold_info = f"Detected: {person_count} people"
            severity_color = "#6f42c1"
        
        subject = f"🚨 {alert_type} ALERT - {camera_name}"
        
        body = f"""{alert_type} ALERT

Camera: {camera_name}
{threshold_info}
Time: {timestamp}{location_text}

This is an automated alert from your InsightEye surveillance system.
The people count has exceeded your configured threshold settings.

Please review the situation and adjust thresholds if necessary.

---
InsightEye Surveillance System
Configure your alert settings in the camera management section.
"""

        html_body = f"""
<html>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
    <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
        <div style="background-color: {severity_color}; color: white; padding: 15px; border-radius: 5px; text-align: center; margin-bottom: 20px;">
            <h1 style="margin: 0; font-size: 24px;">🚨 OCCUPANCY ALERT</h1>
            <h2 style="margin: 5px 0 0 0; font-size: 20px;">{alert_type}</h2>
        </div>
        
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 5px; margin-bottom: 20px;">
            <h3 style="color: {severity_color}; margin-top: 0;">Alert Details:</h3>
            <ul style="list-style: none; padding: 0;">
                <li style="padding: 5px 0;"><strong>Camera:</strong> {camera_name}</li>
                <li style="padding: 5px 0;"><strong>Count:</strong> <span style="color: {severity_color}; font-weight: bold;">{person_count} people</span></li>
                <li style="padding: 5px 0;"><strong>Threshold:</strong> {f'>{greater_than}' if greater_than is not None and person_count > greater_than else f'<{less_than}' if less_than is not None and person_count < less_than else 'N/A'}</li>
                <li style="padding: 5px 0;"><strong>Time:</strong> {timestamp}</li>
            </ul>
            {f'<h4>Location Details:</h4><ul style="list-style: none; padding-left: 20px;">' + ''.join(f'<li style="padding: 2px 0;">• {part}</li>' for part in (location_parts if location_info else [])) + '</ul>' if location_info and location_parts else ''}
        </div>
        
        <div style="background-color: #e7f3ff; border: 1px solid #b3d9ff; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
            <h4 style="color: #0056b3; margin-top: 0;">📊 Occupancy Monitoring</h4>
            <p style="margin-bottom: 0; color: #0056b3;">This alert indicates that the people count has triggered your configured threshold. Please review the situation and adjust settings if needed.</p>
        </div>
        
        <div style="font-size: 12px; color: #6c757d; border-top: 1px solid #dee2e6; padding-top: 15px;">
            <p>This is an automated alert from your InsightEye surveillance system.</p>
            <p>You can configure alert thresholds in the camera management section of your dashboard.</p>
        </div>
    </div>
</body>
</html>
"""

        success = await send_email(user_email, subject, body, html_body=html_body)
        if success:
            logger.info(f"People count alert email sent successfully to {user_email} for {person_count} people at {camera_name}")
        else:
            logger.error(f"Failed to send people count alert email to {user_email} for {person_count} people at {camera_name}")
        return success
        
    except Exception as e:
        logger.error(f"Error sending people count alert email to {user_email}: {e}", exc_info=True)
        return False
       