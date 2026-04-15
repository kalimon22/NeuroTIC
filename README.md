# ⬡ NeuroTIC

**NeuroTIC** is an autonomous, multi-agent ontology engine designed to map, explore, and expand complex knowledge domains. It uses a combination of Graph Databases (CozoDB), Large Language Models (via Ollama), and a dual-agent architecture to build deeply interconnected knowledge graphs from a single seed concept or provided source files.


## 🚀 Key Features

- **Autonomous Knowledge Expansion**: Starting from a "Seed" concept, the engine uses LLM agents to automatically discover and verify new relationships and entities.
- **TBox & ABox Separation**: Implements a structural ontology layer (TBox) for concept definitions and a grounding layer (ABox) for real-world facts and instances.
- **Hybrid Relationship Discovery**: An optimized shortest-path engine (up to 20 levels deep) that treats the graph as undirected and implements Breadth-First Search (BFS) for perfect cycle detection and speed.
- **Interactive Web UI**: Explore the graph, manage multiple ontologies, approve/reject LLM-suggested facts, and ask natural language questions translated into Datalog.
- **Dockerized Environment**: Ready to deploy with a single command, integrating the viewer and the graph engine.

## 🏗️ Architecture

- **Backend**: Python / Flask
- **Database**: [CozoDB](https://www.cozodb.org/) (Datalog-based Graph Database)
- **Model Integration**: Ollama (supports any model, optimized for Llama 3/3.1)
- **Frontend**: Modern Vanilla JS / CSS with a high-performance interactive UI.

## 🛠️ Installation & Setup

NeuroTIC is designed to run in a Docker environment.

### Prerequisites
- [Docker](https://www.docker.com/) & [Docker Compose](https://docs.docker.com/compose/)
- [Ollama](https://ollama.com/) running locally (or reachable via network)

### Deployment

1. **Clone the repository**:
   ```bash
   git clone https://github.com/user/NeuroTIC.git
   cd NeuroTIC
   ```

2. **Configure your environment**:
   Ensure your local Ollama instance is accessible. You may need to adjust the `OLLAMA_BASE_URL` in your configuration if it's not on the default host.

3. **Launch the stack**:
   ```bash
   docker-compose up --build -d
   ```

4. **Access the application**:
   Open your browser and navigate to `http://localhost:5000`.

## 📖 Usage

1. **Create an Ontology**: Go to the "Ontologías" tab, enter a seed name (e.g., "Física Cuántica"), and set the mode to "Seed".
2. **Autonomous Growth**: The system will begin generating a TBox structure and populating it with facts (ABox).
3. **Review Facts**: Facts suggested by the LLM appear in the "Revisiones" tab for manual approval or rejection.
4. **Discover Relationships**: Use the "Relaciones" tab to find how two distantly related concepts (e.g., "Newton" and "Computación Cuántica") are connected through the graph.
5. **Interactive Q&A**: Ask natural language questions in the "Preguntar" tab; the system will attempt to answer using the verified facts in the database.

## 📜 License

This project is licensed under the MIT License - see the LICENSE file for details.

---
*Built with ❤️ for Autonomous Knowledge Discovery.*
