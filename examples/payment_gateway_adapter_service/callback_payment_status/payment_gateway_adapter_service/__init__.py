import config
from helpers import get_payment_provider_request_ordered, HttpSession


service_name = 'PaymentGatewayAdapterService'
requests_session = HttpSession()

requests_session.url = f'https://{config.k8s_host}/service/paymentgatewayadapter/api/'
requests_session.verify = False
requests_session.headers.update({'Content-Type': 'application/json'})

def operation_payment_status_xml(
    client_id: str,
    terminal_id: str,
    *,
    state: str,
    reason_code: str,
    message: str,
) -> str:
    """Типовой колбэк POST .../v1/callback/paymentStatus — корень <operation>."""
    params = [
        ('order_id', '449420'),
        ('id', '413017'),
        ('date', '2021.09.22 16:19:16'),
        ('type', 'P2PTRANSFER'),
        ('state', state),
        ('reason_code', reason_code),
        ('message', message),
        ('name', 'P2P Transfer'),
        ('pan', '411111******1111'),
        ('pan2', '557071******4487'),
        ('amount', '10000'),
        ('fee', '3000'),
        ('currency', '643'),
        ('approval_code', '264214'),
        ('sector_id', '3013'),
        ('expdate', '04/2025'),
        ('ps', '1'),
        ('rrn', '27990043'),
        ('terminal_id', terminal_id),
        ('to_client_ref', client_id),
    ]
    body, signature = get_payment_provider_request_ordered(params)
    return f'<operation>\n{body}<signature>{signature}</signature>\n</operation>'


def card_operation_ext_ops_xml(
    client_id: str,
    term_id: str,
    *,
    trancode: str,
    type_: str,
    reason_code: str,
) -> str:
    """Внешний колбэк POST .../v1/callback/paymentStatus/extOps — корень <card_operation>."""
    params = [
        ('sector', '7803'),
        ('client_ref', client_id),
        ('personID', '585172'),
        ('mcc', '4814'),
        ('amount', '10300'),
        ('aqfee', '0'),
        ('currency', '643'),
        ('trancode', trancode),
        ('TWOID', '100000000000'),
        ('dateTime', '2025-08-20 17:30:50'),
        ('credLimit', '0'),
        ('type', type_),
        ('reasonCode', reason_code),
        ('maskedPan', '220441******2763'),
        ('termID', term_id),
        ('termLocation', 'Moskva, ulitca 20'),
        ('retailerID', '100000000000'),
        ('retailerName', 'p2p renic'),
        ('country', '643'),
        ('avaliableBalance', '829400'),
    ]
    body, signature = get_payment_provider_request_ordered(params)
    return f'<card_operation>\n{body}<signature>{signature}</signature>\n</card_operation>'
