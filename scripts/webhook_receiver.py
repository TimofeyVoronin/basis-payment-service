from fastapi import FastAPI, Response

from app.schemas import PaymentWebhook

app = FastAPI()


@app.post("/webhook", status_code=204)
async def receive_webhook(payload: PaymentWebhook) -> Response:
    print(payload.model_dump(mode="json"), flush=True)
    return Response(status_code=204)
