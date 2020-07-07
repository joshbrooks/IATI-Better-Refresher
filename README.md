# IATI-Better-Refresher
An experimental smart IATI Registry Refresher

## Setup
```
python3 -m virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Operation
```
python3 refresh.py
python3 reload.py
# To retry errored downloads
python3 reload.py -e
```

## To do

- Better logging
- More optimization
