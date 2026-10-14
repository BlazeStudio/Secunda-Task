import asyncio

from app.consumer.app import create_app

if __name__ == "__main__":
    asyncio.run(create_app().run())
