from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status

from app.api.dependencies import SessionDep, verify_api_key
from app.schemas import PaymentAccepted, PaymentCreate, PaymentRead
from app.services import IdempotencyKeyReusedError, PaymentService

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
    responses={status.HTTP_401_UNAUTHORIZED: {"description": "Invalid or missing API key"}},
)

IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
        description="Unique key of the operation; retries with the same key return "
        "the originally created payment",
    ),
]


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_409_CONFLICT: {
            "description": "Idempotency-Key was already used with a different request body"
        }
    },
)
async def create_payment(
    body: PaymentCreate,
    idempotency_key: IdempotencyKey,
    session: SessionDep,
    response: Response,
) -> PaymentAccepted:
    try:
        result = await PaymentService(session).create(body, idempotency_key)
    except IdempotencyKeyReusedError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key has already been used with a different request body",
        ) from None

    payment = result.payment
    response.headers["Location"] = router.url_path_for("get_payment", payment_id=str(payment.id))
    if not result.created:
        response.headers["Idempotent-Replayed"] = "true"
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get(
    "/{payment_id}",
    responses={status.HTTP_404_NOT_FOUND: {"description": "Payment not found"}},
)
async def get_payment(payment_id: UUID, session: SessionDep) -> PaymentRead:
    payment = await PaymentService(session).get(payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return PaymentRead.model_validate(payment)
