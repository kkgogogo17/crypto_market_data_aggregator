FROM python:3.11

WORKDIR /app

# Install uv (ultrafast Python manager)
RUN pip install uv

# Copy your entire project (including pyproject.toml, source, etc.)
COPY . .

# Install dependencies using uv; this reads pyproject.toml automatically
RUN uv pip install --system .
RUN uv pip install --system .[test]

# Optionally, if you want editable mode (for development), use:
# RUN uv pip install --system --editable .

# Run your main app
CMD ["python", "src/main.py"]
