
# Type aliases
from typing import Dict


JsonDict = Dict[str, any]


class PaymentTypes:
    """Payment type mappings."""
    CASH_OPERATOR = 0
    BANK_OPERATOR = 1
    CASH_PARKOMETER = 2
    BANK_PARKOMETER = 3
    MOBILE_APP = 4

    @classmethod
    def get_description(cls, payment_type: int) -> str:
        """Get human-readable payment type description."""
        mappings = {
            cls.CASH_OPERATOR: 'нал оператор',
            cls.BANK_OPERATOR: 'банк оператор',
            cls.CASH_PARKOMETER: 'нал паркомат',
            cls.BANK_PARKOMETER: 'банк паркомат',
            cls.MOBILE_APP: 'мобильное приложение'
        }

        return mappings.get(payment_type, 'unknown')


class SendToTypes:
    """Send-to destination types."""
    PRINT = 'print'
    POWERBI = 'powerbi'
    BILLS = 'bills'


class BillPaymentTypes:
    """Bill payment types."""
    CASH = 'CASH'
    CARD = 'CARD'

    @classmethod
    def get_type(cls, payment_type: int) -> str:
        """Get human-readable bill payment type description."""
        mappings = {
            PaymentTypes.CASH_OPERATOR: cls.CASH,
            PaymentTypes.BANK_OPERATOR: cls.CARD,
            PaymentTypes.CASH_PARKOMETER: cls.CASH,
            PaymentTypes.BANK_PARKOMETER: cls.CARD,
            PaymentTypes.MOBILE_APP: cls.CARD
        }

        return mappings.get(payment_type, cls.CASH)
