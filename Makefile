.PHONY: build run test

build: env lid.176.bin data.db

run: build
	env/bin/python -m crawler

test: build
	env/bin/python -m pytest

env: requirements.txt
	touch -c env
	test -d env || python -m venv env
	env/bin/pip install -r requirements.txt

lid.176.bin:
	wget https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin

data.db: env crawler/db.py
	touch -c $@
	env/bin/python -m crawler.db
