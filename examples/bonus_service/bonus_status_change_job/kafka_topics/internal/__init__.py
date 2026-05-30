from hamcrest import not_none, equal_to, matches_regexp
from kafka.consumer.fetcher import ConsumerRecord

from helpers import get_test_uuid, check_assert_that


class BaseTopic:
    @staticmethod
    def _get_headers_from_message(message: ConsumerRecord):
        return {k: v.decode() for k, v in message.headers if v}

    def _check_headers(self, message: ConsumerRecord, producer_name):
        headers = self._get_headers_from_message(message)
        check_assert_that(headers.get('ProducerName'), equal_to(producer_name), 'Ошибка в ProducerName')
        check_assert_that(headers.get('traceparent'), not_none(), 'Ошибка в traceparent')
        check_assert_that(headers.get('traceparent'),
                          matches_regexp('^00-[0-9a-f]{32}-[0-9a-f]{16}-00$'), 'Ошибка в traceparent')
        check_assert_that(headers.get('X-Correlation-ID'), not_none(), 'Ошибка в X-Correlation-ID')
        check_assert_that(headers.get('X-Correlation-ID'),
                          matches_regexp('^[0-9a-f]{32}$'), 'Ошибка в X-Correlation-ID')

    def _set_headers(self):
        headers = {
            'ProducerName': 'Portfolio.Autotests',
            'traceparent': f'traceparent\t00-{get_test_uuid().replace('-', '')}-ffffffffffffffff-00',
            'X-Correlation-ID': get_test_uuid().replace('-', '')
        }
        return [(k, v.encode()) for k, v in headers.items()]
