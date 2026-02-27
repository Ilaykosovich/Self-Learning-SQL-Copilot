# Self-Learning SQL Copilot

A conversational, schema-adaptive AI system that lets users work with relational databases using **natural language**, without any prior knowledge of SQL or the underlying schema.

**Detailed project description and design:**  
[Text-to-SQL: Designing an Adaptive AI Agent (Medium)](https://medium.com/@ilaykosovich/text-to-sql-designing-an-adaptive-ai-agent-part-2-0a364e7cc550)

---

## Getting Started (Docker Recommended)

### 1. Environment Configuration

Copy the example environment file:

```bash
cp .env.example .env
```

Edit `.env` as needed.

### 2. Build Docker Image

From the project root:

```bash
docker build -t self-learning-sql-copilot .
```

### 3. Run the Container

```bash
docker run -p 8000:8000 --env-file .env self-learning-sql-copilot
```

The service will be available at [http://localhost:8000](http://localhost:8000)

---

### 4. Setup Required RAG Service

You **must** set up and run the RAG (Retrieval-Augmented Generation) service, which manages all connections to your databases.

- Download and configure the [RagDbService project](https://github.com/Ilaykosovich/RagDbService) according to its instructions.
- Ensure RagDbService is running and properly connected to your database(s) before using the SQL Copilot.

---

## Key Features

- Conversational interface: natural language to SQL
- **No need** to manually describe database schema
- Dynamic schema introspection (tables, columns, relations)
- Iterative SQL refinement (error-driven)
- (Optional) Show generated SQL to user (transparency)
- Session-based chat flow
- Observability: Prometheus + Grafana
- Supports both local and cloud-based LLMs (OpenAI, Ollama, etc.)

---

## Architecture

- **Backend:** FastAPI (chat session, orchestration, SQL execution)
- **Frontend:** Minimal single-page web client
- **LLM Runtime:** OpenAI or local (Ollama, etc)
- **Observability:** Prometheus, Grafana

> *For a step-by-step architectural walkthrough, see the [Medium article](https://medium.com/@ilaykosovich/text-to-sql-designing-an-adaptive-ai-agent-part-2-0a364e7cc550).*

<p align="center">
  <img src="images/Adaptive-Memory-AugmentedDB.drawio.svg" width="900" />
  <img src="images/main_page.png" width="900" />
  <img src="images/generated_sql.png" width="900" />
</p>

---

## LLM Configuration

Configure in `.env` to use either **OpenAI** or local models (Ollama, etc.)

---

## Status

🚧 **Work in progress**

---

## License

Your license info here

---

