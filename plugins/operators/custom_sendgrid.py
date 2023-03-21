import os
from typing import Iterable, Union, Sequence

from sendgrid.helpers.mail import (
    Content,
    Email,
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
    """


    Parameters
    ----------
    :param to: List of email addresses to send the email to. Can be a string or list of strings.
        :type to: Union[str, Sequence[str]]
    :param subject: Subject of the email.
        :type subject: str
    :param cc: List of email addresses to CC. Optional.
        :type cc: Union[str, Sequence[str]], optional
    :param bcc: List of email addresses to BCC. Optional.
        :type bcc: Union[str, Sequence[str]], optional
    :param sandbox_mode: Whether to send the email in sandbox mode. Default is False.
        :type sandbox_mode: bool, optional
    :param sendgrid_conn_id: The connection ID to use when sending email through SendGrid. Default is 'sendgrid-default'.
        :type sendgrid_conn_id: str, optional
    :param is_multiple: Whether the email is being sent to multiple recipients. Default is True.
        :type is_multiple: bool, optional
    :param parameters: Additional parameters to be passed to SendGrid API. Optional.
        :type parameters: Dict, optional

    :raises Exception: if `html_content` is not included in `parameters`.

    """

    template_fields: Sequence[str] = ("subject", "parameters", "to")
    template_ext: Sequence[str] = (".json")

    def __init__(
        self,
        *,
        to: AddressesType,
        subject: str,
        cc: AddressesType | None = None,
        bcc: AddressesType | None = None,
        sandbox_mode: bool = False,
        sendgrid_conn_id: str = "sendgrid-default",
        is_multiple: bool = True,
        parameters: dict | None = None,
        **kwargs,
    ) -> None:
        self.to = to
        self.subject = subject
        self.cc = cc
        self.bcc = bcc
        self.sandbox_mode = sandbox_mode
        self.sendgrid_conn_id = sendgrid_conn_id
        self.parameters = parameters or {}
        self.is_multiple = is_multiple
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:

        mail = Mail(is_multiple=self.is_multiple)

        if "from_email" not in self.parameters:
            self.parameters["from_email"] = os.environ.get("SENDGRID_MAIL_FROM")

        if "from_name" not in self.parameters:
            self.parameters["from_name"] = os.environ.get("SENDGRID_MAIL_SENDER")

        from_email = self.parameters.pop("from_email")
        from_name = self.parameters.pop("from_name")

        mail.from_email = Email(from_email, from_name)
        mail.subject = self.subject
        mail.mail_settings = MailSettings()

        # FIXME : SINCE IT IS INSIDE THE parameters. IT IS FUCKED WHEN RENDERING.

        if self.sandbox_mode:
            mail.mail_settings.sandbox_mode = SandBoxMode(enable=True)

            # We could not have this. But since we are mostly going to use this Operator for sending HTML content
            # email, this way we are sure that we are not sending empty e-mails in case of a failure that
            # was not intercepted before.
        html_content = self.parameters['html_content']
        mail.add_content(Content("text/html",html_content ))
        
        # TODO : TRY TO ACCESS USING context.params

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

        for key in self.parameters.keys():
            setattr(mail, key, self.parameters[key])

        _post_sendgrid_mail(mail.get(), self.sendgrid_conn_id)


    def render_template_fields(self, context, jinja_env=None) -> None:
        """Add the rendered 'parameters' to the context dictionary before running the templating"""
        # Like the original method, get the env if not provided
        if not jinja_env:
            jinja_env = self.get_template_env()

        # Run the render template on parameters and add it to the context
        if self.parameters:
            if 'html_content' not in self.parameters:
                raise Exception("This SendgridOperator requires an html_content in parameters.")

            context["params"] = self.render_template(
                self.parameters, context, jinja_env, set()
            )

        # Call the original method
        super().render_template_fields(context=context, jinja_env=jinja_env)


