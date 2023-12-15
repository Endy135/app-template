"""Mailer class to send emails."""
import os
import smtplib
import mimetypes
from email import encoders
from email.message import EmailMessage
from email.mime.text import MIMEText
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.utils import make_msgid
from typing import Optional, List, Dict

from .app import AppConfig


class Mailer(AppConfig):
    """Create a mailer object to send emails."""

    _media_path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "resources", "media"
        )
    )

    def __init__(self):
        """Initialize mailer object."""
        AppConfig.__init__(self)

    def send(
        self,
        subject: str,
        message: str,
        to_recipients: Optional[List[str]] = [],
        cc_recipients: Optional[List[str]] = [],
        bcc_recipients: Optional[List[str]] = [],
        attachments: Optional[List[str]] = [],
        attachments_inline: Optional[List[str]] = [],
        links: Optional[Dict[str, str]] = {},
    ):
        """Send email message.

        Parameters
        ----------
        subject : str
            Email subject.
        message : str
            Email body. It can be either in plain text or in html syntax.
        to_recipients: list of str, optional
            Main recipients of the email.
        cc_recipients: list of str, optional
            Cc recipients of the email.
        bcc_recipients: list of str, optional
            Bcc recipients of the email.
        attachments: list of str, optional
            List of abspaths to files to be attached to the message.
        attachments_inline: list of str, optional
            List of abspaths to files to be displayed in the message.
        links: dict, optional.
            Dictionary of hyperlinks to display. Key is label, value is the URL to include.

        """
        # Fixed params header & foot images
        t_head_pic = (
            os.path.join(self._media_path, "Banner-Stellantis.png"),
            make_msgid()
        )
        # t_foot_pic = ("", make_msgid())

        # Prepare inline attachments of hmtl code
        dict_attachments_inline = {}
        s_img = ""
        if len(attachments_inline) != 0:
            # Add inline attachments
            for i in range(len(attachments_inline)):
                dict_attachments_inline[i] = (attachments_inline[i], make_msgid())
                s_img += f"<img src='cid:{dict_attachments_inline[i][1][1:-1]}'>\n"
            # Add header & footer pic
            dict_attachments_inline[i + 1] = t_head_pic
        else:
            # Add header & footer pic
            dict_attachments_inline[0] = t_head_pic

        # Prepare hyperlink integration
        s_hlink = ""
        if len(links) != 0:
            for label, url in links.items():
                s_hlink += f"<a href={url}>{label}</a> <br>"
            s_hlink += "<br>"

        # Initialise email
        msg = EmailMessage()
        msg.set_content(
            f"""
        <html>
            <img src='cid:{t_head_pic[1][1:-1]}'>
            <body>
                <p>{message}</p>
               {s_hlink}
               {s_img}
            </body>
        </html>
        """.replace(
                "\n", "<br>"
            ),
            subtype="html",
        )

        # Open the image and display it inside the email
        for code, t in dict_attachments_inline.items():
            path, image_cid = t
            with open(path, "rb") as img:
                # Know the Content-Type of the image
                maintype, subtype = mimetypes.guess_type(img.name)[0].split('/')
                # Attach it
                msg.add_related(img.read(), maintype=maintype, subtype=subtype, cid=image_cid)

        # Recipients
        msg["Subject"] = subject.strip()
        msg["From"] = self.mail_config["sender"]
        if to_recipients == []:
            to_recipients = self.mail_config["to_recipients"]
        msg["To"] = "; ".join(to_recipients)

        if cc_recipients == []:
            cc_recipients = self.mail_config["cc_recipients"]
        msg["Cc"] = "; ".join(cc_recipients)

        if bcc_recipients != []:
            msg["Bcc"] = "; ".join(bcc_recipients)

        # Add attachments
        for path in attachments:
            self.add_attachment_to_message(msg, path)

        # Send email
        with smtplib.SMTP(self.mail_config["host"]) as s:
            s.starttls()
            s.ehlo()
            s.login(self.mail_config["user"], self.mail_config["password"])
            all_recipients = [*to_recipients, *cc_recipients, *bcc_recipients]
            s.sendmail(self.mail_config["sender"], all_recipients, msg.as_string())

    @staticmethod
    def add_attachment_to_message(main_message, attachment_path):
        """Attach file (from absolute path) to a MIMEMultipart message object."""
        if not os.path.isfile(attachment_path):
            raise ValueError(f"Wrong path provided as attachment: {attachment_path}")
        filename = os.path.basename(attachment_path)

        # Guess the content type based on the file"s extension. Encoding
        # will be ignored, although we should check for simple things like
        # gzip"d or compressed files.
        ctype, encoding = mimetypes.guess_type(attachment_path)
        if ctype is None or encoding is not None:
            # No guess could be made, or the file is encoded (compressed), so
            # use a generic bag-of-bits type.
            ctype = "application/octet-stream"
        maintype, subtype = ctype.split("/", 1)
        if maintype == "text":
            fp = open(attachment_path)
            # Note: we should handle calculating the charset
            msg = MIMEText(fp.read(), _subtype=subtype)
            fp.close()
        elif maintype == "image":
            fp = open(attachment_path, "rb")
            msg = MIMEImage(fp.read(), _subtype=subtype)
            fp.close()
        elif maintype == "audio":
            fp = open(attachment_path, "rb")
            msg = MIMEAudio(fp.read(), _subtype=subtype)
            fp.close()
        else:
            fp = open(attachment_path, "rb")
            msg = MIMEBase(maintype, subtype)
            msg.set_payload(fp.read())
            fp.close()
            # Encode the payload using Base64
            encoders.encode_base64(msg)

        # Set the filename parameter
        msg.add_header("Content-Disposition", "attachment", filename=filename)
        main_message.attach(msg)
