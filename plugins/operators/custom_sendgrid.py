import os
from typing import Iterable, Union, Sequence

from sendgrid.helpers.mail import (
    Asm,
    Bcc,
    Cc,
    Content,
    Email,
    GroupId,
    Mail,
    MailSettings,
    Personalization,
    SandBoxMode,
)

from airflow.models.baseoperator import BaseOperator
from airflow.providers.sendgrid.utils.emailer import _post_sendgrid_mail
from airflow.utils.context import Context
from airflow.utils.email import get_email_address_list

AddressesType = Union[str, Iterable[str]]


class SendGridOperator(BaseOperator):
    template_fields: Sequence[str] = ("subject", "html_content", "to")

    def __init__(
        self,
        *,
        to: AddressesType,
        subject: str,
        html_content: str,
        files: AddressesType | None = None,
        cc: AddressesType | None = None,
        bcc: AddressesType | None = None,
        sandbox_mode: bool = False,
        conn_id: str = "sendgrid_default",
        is_multiple: bool = True,
        extra_arguments: dict,
        **kwargs,
    ) -> None:
        self.to = to
        self.subject = subject
        self.html_content = html_content
        self.files = files
        self.cc = cc
        self.bcc = bcc
        self.sandbox_mode = sandbox_mode
        self.conn_id = conn_id
        self.extra_arguments = extra_arguments or {}
        self.is_multiple = is_multiple
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:

        mail = Mail(is_multiple=self.is_multiple)

        from_email = self.extra_arguments.get("from_email") or os.environ.get(
            "SENDGRID_MAIL_FROM"
        )
        from_name = self.extra_arguments.get("from_name") or os.environ.get(
            "SENDGRID_MAIL_SENDER"
        )

        mail.from_email = Email(from_email, from_name)
        mail.subject = self.subject
        mail.mail_settings = MailSettings()

        if self.sandbox_mode:
            mail.mail_settings.sandbox_mode = SandBoxMode(enable=True)

        mail.add_content(Content("text/html", self.html_content))

        # Add the recipient list of to emails.
        personalization = Personalization()
        to = get_email_address_list(self.to)
        for to_address in to:
            personalization.add_to(Email(to_address))
        if self.cc:
            cc = get_email_address_list(self.cc)
            for cc_address in cc:
                personalization.add_cc(Email(cc_address))
        if self.bcc:
            bcc = get_email_address_list(self.bcc)
            for bcc_address in bcc:
                personalization.add_bcc(Email(bcc_address))

        group_id = self.extra_arguments.get("GroupId")

        if group_id:
            mail.asm = Asm(GroupId(group_id))

        _post_sendgrid_mail(mail.get(), self.conn_id)
