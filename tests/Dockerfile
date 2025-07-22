FROM educationwarehouse/edwhale-3.13:latest

WORKDIR /srv

COPY pyproject.toml .

RUN uv pip install -e .[dev,migrate]

USER microservices
