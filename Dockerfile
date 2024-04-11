# imagen necesaria
FROM python:3.9

# establecer directorio
WORKDIR /app

# copiar archivo de requerimientos al contenedor
COPY . .

# instalar dependencias
RUN pip install -r requirements.txt

