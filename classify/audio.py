import requests

import os
import auth


class VoiceTranscriber:
    def __init__(self):
        self.token = None
        self.refresh_token()

    def refresh_token(self):
        self.token = auth.get_token_salut()

    def transcribe_audio(self, audio_path):
        # Проверяем, что токен есть
        url = "https://smartspeech.sber.ru/rest/v1/speech:recognize"

        with open(audio_path, 'rb') as audio_file:
            files = {'file': (os.path.basename(audio_path), audio_file, 'audio/mpeg')}
            headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'audio/mpeg;codecs=opus'
            }

            response = requests.post(url, headers=headers, files=files,verify=False)

        if response.status_code == 200:
            return response.json()['result'][0]
        else:
            # Если токен просрочен, попробуем еще раз с новым токеном
            if response.status_code == 401:
                self.refresh_token()
                return self.transcribe_audio(audio_path)
            else:
                raise Exception(f"Ошибка транскрибации: {response.text}")


