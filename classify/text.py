import requests
import auth
import logging


class TextClassifier:
    def __init__(self):
        self.token = None
        self.refresh_token()

    def refresh_token(self):
        self.token = auth.get_token()

    def classify_text(self, msg):
        chat_url = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"
        chat_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json", "Authorization": f"Bearer {self.token}"
        }

        chat_data = {
            "model": "GigaChat",  # Укажите нужную модель
            "messages": [
                {
                    "role": "user",
                    "content": f"Определи тональность следующего текста: положительная, отрицательная или нейтральная. "+
                               f"Ответь только одним словом: 'positive', 'negative' или 'neutral'.\n\nТекст: {msg}"
                }
            ],
            "temperature": 0.1,
            "max_tokens": 1000
        }

        # 3. Отправка запроса
        chat_response = requests.post(
            chat_url,
            headers=chat_headers,
            json=chat_data,
            verify=False
        )
        if chat_response.status_code == 429:
            self.refresh_token()
            chat_response = requests.post(
                chat_url,
                headers=chat_headers,
                json=chat_data,
                verify=False
            )

        if chat_response.status_code == 200:
            result = chat_response.json()
            return result["choices"][0]["message"]["content"]
        else:
            return None
