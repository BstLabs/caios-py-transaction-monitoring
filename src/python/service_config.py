from jdict import jdict
from caios.service.config_base import get_default_config_base
from caios.abc.mutable_mapping import Mapping, MutableMapping
from caios.abc.mutable_storage import MutableStorage
from caios.protocol.channel import Channel
from service import TransactionMonitoring


def get_configuration(service_name: str, mode: str) -> tuple[jdict, ...]:
    """
    Return a default configuration for development
    """
    return (
        *get_default_config_base(), 
        jdict(
            resource='GSpread',
            interface='Mapping',
            secret='name of the secret',
            scope=['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'],
            name='transactions',
            workbook='TransactionMonitoring'
        ),
        jdict(
            resource='s3',
            bucket='${UserStorage}',
            folder='${ServiceFolder}/transactions',     
            interface=MutableMapping,
            name='service_storage'
        ),
        jdict(
            resource='s3',
            bucket='${UserStorage}',
            folder='${ServiceFolder}/anomalies',     
            interface=MutableMapping,
            name='alerts_storage'
        ),
        jdict(
            resource="SES",
            interface=Channel,
            name="notification_mail",
            sender="sender email"
        ),
        jdict(
            resource='Events',
            interface='CronJob',
            expressions = (
                jdict(
                    expression="0/1 * * * ? *", 
                    target=TransactionMonitoring._check_transactions,
                    args=dict(
                        recipient="recipient email",
                        window_length=7
                    ), 
                    enable=True
                ),
            ),
            name="daily_check"
        )
    )
