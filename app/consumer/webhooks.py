import httpx

from app.models import Payment
from app.schemas import PaymentRead


class WebhookDeliveryError(Exception):
    pass


class WebhookSender:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def send(self, payment: Payment) -> None:
        body = PaymentRead.model_validate(payment).model_dump(mode="json")
        try:
            response = await self._client.post(payment.webhook_url, json=body)
            # anything but 2xx counts as a failed delivery, redirects included
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f"webhook delivery failed: {exc!r}") from exc
