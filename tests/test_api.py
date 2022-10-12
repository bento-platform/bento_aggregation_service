import json

from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application

from bento_aggregation_service.app import application
from bento_aggregation_service.constants import SERVICE_ID


class ApiTests(AsyncHTTPTestCase):
    def get_app(self) -> Application:
        return application

    def test_service_info(self):
        r = self.fetch("/service-info")
        self.assertEqual(r.code, 200)
        d = json.loads(r.body)
        self.assertEqual(d["id"], SERVICE_ID)
        # TODO: More rigourous test
