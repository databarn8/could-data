# Advanced RAG System with Reranking

A sophisticated Retrieval-Augmented Generation (RAG) system that extracts information from PDF documents using remote Ollama models and ChromaDB, featuring advanced reranking capabilities for improved relevance.

## System Architecture

**Distributed Setup:**
- **Ubuntu Server** (192.168.1.253): Runs Ollama and ChromaDB services
- **Windows Client**: Runs Python RAG application and processes PDFs
- **Network Communication**: Client connects to remote services via REST APIs

```
Windows Client                    Ubuntu Server (192.168.1.253)
┌─────────────────┐              ┌──────────────────────────────┐
│ Python RAG App  │─────────────▶│ Ollama (port 11434)          │
│ PDF Processing  │              │ - mxbai-embed-large          │
│ Query Interface │              │ - Qwen3-Reranker-8B          │
└─────────────────┘              │ - qwen2.5:7b-instruct       │
                                 │                              │
                                 │ ChromaDB (port 8000)         │
                                 │ - Vector storage             │
                                 │ - Similarity search          │
                                 └──────────────────────────────┘
```

## Features

### Core Capabilities
- **PDF Text Extraction**: Extracts and processes text from PDF documents using pdfplumber
- **Intelligent Chunking**: Uses RecursiveCharacterTextSplitter for semantic text segmentation
- **Vector Storage**: Stores embeddings in remote ChromaDB for fast similarity search
- **Advanced Reranking**: Uses specialized reranker models to improve retrieval relevance
- **Flexible Output**: Raw results or AI-formatted tables and summaries

### Remote Architecture
- **Remote Ollama Server**: All AI models run on a dedicated remote server
- **Remote ChromaDB**: Vector database hosted separately for scalability
- **Network Optimized**: Designed for distributed deployment

### AI Models Integration
- **Embeddings**: `mxbai-embed-large` for high-quality vector representations
- **Reranking**: `dengcao/Qwen3-Reranker-8B:F16` for relevance scoring
- **Text Generation**: `qwen2.5:7b-instruct` for table formatting and summaries

## System Architecture

```
PDF Document → Text Extraction → Chunking → Embedding → ChromaDB
                                                            ↓
Query → Initial Retrieval (10 docs) → Reranking → Top 5 → LLM Formatting
```

## Prerequisites

### Ubuntu Server Setup (192.168.1.253)

#### Install Ollama
```bash
# Install Ollama on Ubuntu
curl -fsSL https://ollama.ai/install.sh | sh

# Start Ollama service
sudo systemctl start ollama
sudo systemctl enable ollama

# Configure Ollama to accept connections from other machines
sudo systemctl edit ollama
# Add these lines:
[Service]
Environment="OLLAMA_HOST=0.0.0.0:11434"

# Restart Ollama
sudo systemctl restart ollama

# Verify Ollama is running
curl http://localhost:11434/api/tags
```

#### Install Required Models
```bash
# Pull embedding model (669MB)
ollama pull mxbai-embed-large

# Pull reranker model (8GB+)
ollama pull dengcao/Qwen3-Reranker-8B:F16

# Pull text generation model (7GB+)
ollama pull qwen2.5:7b-instruct

# Verify models are installed
ollama list
```

#### Install and Configure ChromaDB
```bash
# Install ChromaDB server
pip install chromadb

# Start ChromaDB server (accessible from network)
chroma run --host 0.0.0.0 --port 8000 --path /path/to/chroma/data

# Or run as a service (create systemd service file)
sudo nano /etc/systemd/system/chromadb.service
```

Example ChromaDB service file:
```ini
[Unit]
Description=ChromaDB Server
After=network.target

[Service]
Type=exec
User=your-username
WorkingDirectory=/home/your-username
ExecStart=/usr/local/bin/chroma run --host 0.0.0.0 --port 8000 --path /home/your-username/chroma-data
Restart=always

[Install]
WantedBy=multi-user.target
```

#### Firewall Configuration
```bash
# Allow Ollama port (11434)
sudo ufw allow 11434/tcp

# Allow ChromaDB port (8000)  
sudo ufw allow 8000/tcp

# Check firewall status
sudo ufw status
```

### Windows Client Setup

#### Python Environment
```cmd
# Create virtual environment (optional but recommended)
python -m venv rag-env
rag-env\Scripts\activate

# Install required packages
pip install pdfplumber chromadb langchain langchain-ollama requests

# Verify installation
python -c "import chromadb, pdfplumber; print('Dependencies installed successfully')"
```

#### Network Connectivity Test
```cmd
# Test Ollama connection from Windows
curl http://192.168.1.253:11434/api/tags

# Test ChromaDB connection from Windows  
curl http://192.168.1.253:8000/api/v1/heartbeat
```

## Configuration

Update the remote server IP address in the script:
```python
# Change this to your remote server IP
REMOTE_SERVER_IP = "192.168.1.253"
OLLAMA_PORT = "11434"
CHROMA_PORT = "8000"
```

## Usage

### Basic Usage
```bash
python test_rag_clean.py <pdf_file> <query>
```

### Table Formatting
```bash
python test_rag_clean.py <pdf_file> <query> "table"
```

### Disable Reranking
```bash
python test_rag_clean.py <pdf_file> <query> --no-rerank
```

### Examples
```bash
# Basic query
python test_rag_clean.py apple_10k.pdf "What is Apple's revenue in 2018?"

# Generate formatted tables
python test_rag_clean.py apple_10k.pdf "Sales Data" "table"

# Query without reranking
python test_rag_clean.py apple_10k.pdf "iPhone sales" --no-rerank

# Unit sales analysis
python test_rag_clean.py apple_10k.pdf "Unit sales by product" "table"
```

## How It Works

### 1. Document Processing
- Extracts text from PDF using pdfplumber
- Splits text into overlapping chunks (1000 chars, 100 overlap)
- Handles multiple pages and complex layouts

### 2. Vector Storage
- Converts text chunks to embeddings using `mxbai-embed-large`
- Stores vectors in remote ChromaDB with collection management
- Automatically handles collection cleanup and recreation

### 3. Retrieval Pipeline
- **Initial Search**: ChromaDB similarity search retrieves 10 candidates
- **Reranking**: Qwen3-Reranker-8B scores each document for query relevance
- **Selection**: Returns top 5 most relevant documents
- **Formatting**: qwen2.5:7b-instruct formats results into tables or summaries

### 4. Output Options
- **Raw Text**: Direct chunks from the document
- **Formatted Tables**: AI-generated markdown tables with clean structure
- **Summaries**: Contextual answers to specific questions

## Output Examples

### Geographic Sales Data
```markdown
## Table 1: Net Sales by Geographic Segment
| Region       | Rev (M) | Chg % | Pct |
|--------------|---------|-------|-----|
| Americas     | 112,093 | 16    | 42  |
| Europe       | 62,420  | 14    | 24  |
| Greater China| 51,942  | 16    | 19  |
```

### Product Performance
```markdown
## Table 2: Net Sales by Product Category  
| Product      | Rev (M) | Chg % | Pct |
|--------------|---------|-------|-----|
| iPhone       | 166,699 | 18    | 63  |
| Services     | 37,190  | 24    | 14  |
| Mac          | 25,484  | -1    | 10  |
```

## Advanced Features

### Reranking Benefits
- **Improved Relevance**: Cross-encoder models better understand query-document relationships
- **Context Awareness**: Considers semantic meaning beyond keyword matching  
- **Quality Filtering**: Removes less relevant results even if they have keyword matches

### Table Formatting
- **Compact Design**: Multiple narrow tables instead of wide ones
- **Abbreviated Headers**: "Rev" for Revenue, "Chg" for Change
- **Logical Grouping**: Geographic, Product, and Unit sales separated
- **Consistent Formatting**: Standardized number formats and percentages

### Error Handling
- **Connection Recovery**: Graceful handling of network issues
- **Model Fallbacks**: Continues with original ranking if reranking fails
- **Timeout Management**: Prevents hanging on slow model responses

## Troubleshooting

### Connection Issues
```bash
# Test ChromaDB connection
curl http://192.168.1.253:8000/api/v1/heartbeat

# Test Ollama connection  
curl http://192.168.1.253:11434/api/tags
```

### Common Issues
1. **Model Not Found**: Ensure all models are pulled on remote server
2. **Connection Timeout**: Check firewall settings and network connectivity
3. **Memory Issues**: Large PDFs may require chunking adjustment
4. **Deprecation Warnings**: Install `langchain-ollama` package

### Performance Optimization
- **Chunk Size**: Adjust for document type (1000 chars default)
- **Retrieval Count**: Increase initial retrieval for better reranking
- **Model Selection**: Use smaller models for faster processing

## Customization

### Model Configuration
```python
# Change embedding model
embeddings = OllamaEmbeddings(
    model="your-embedding-model",
    base_url="http://your-server:11434"
)

# Change reranker model
reranker_model = "your-reranker-model"

# Change formatting model
llm = OllamaLLM(
    model="your-llm-model",
    base_url="http://your-server:11434"
)
```

### Chunking Strategy
```python
splitter = RecursiveCharacterTextSplitter(
    chunk_size=1500,      # Increase for longer context
    chunk_overlap=150,    # Adjust overlap
    separators=["\n\n", "\n", ".", "!", "?", " "]
)
```

## Performance Metrics

### Typical Processing Times
- **PDF Extraction**: 1-2 seconds per MB
- **Embedding Creation**: 5-10 seconds for 400 chunks
- **Initial Retrieval**: < 1 second
- **Reranking**: 10-30 seconds (depends on model size)
- **Table Formatting**: 5-15 seconds

### Resource Usage
- **ChromaDB**: ~100MB RAM + vector storage
- **Ollama Models**: 
  - mxbai-embed-large: 669MB
  - Qwen3-Reranker-8B: 8GB+ RAM
  - qwen2.5:7b-instruct: 7GB+ RAM

## License

Open source - modify and distribute as needed.

## Contributing

1. Test with different document types
2. Experiment with model combinations  
3. Optimize prompts for better table formatting
4. Add support for additional file formats

## Support

Check logs for detailed error messages and ensure all remote services are running and accessible.