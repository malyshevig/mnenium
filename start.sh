alias python=/Users/im/imavito/report/.venv/bin/python

python -m bot2.bot > ./logs/bot1.log 2>&1 &
python -m bot2.bot > ./logs/bot2.log 2>&1 &


python -m message_service.app > ./logs/service.log 2>&1 &
python -m classify.classify > ./logs/classify1.log 2>&1 &
python -m classify.classify > ./logs/classify2.log 2>&1 &
python -m orchestrator.orc  > ./logs/orc.log 2>&1 &

