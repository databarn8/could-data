import sys
from pathlib import Path
import pdfplumber
import chromadb
import requests
import json
import re
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import Chroma

# Use the new langchain-ollama package to avoid deprecation warnings
try:
    from langchain_ollama import OllamaEmbeddings, OllamaLLM
except ImportError:
    print("Installing langchain-ollama package...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-U", "langchain-ollama"])
    from langchain_ollama import OllamaEmbeddings, OllamaLLM

# ---------------------------
# PDF text extraction
# ---------------------------
def extract_text_from_pdf(pdf_path: str) -> str:
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
    return text

# ---------------------------
# Smart chunking
# ---------------------------
def smart_chunk_text(text: str, chunk_size=1000, chunk_overlap=100) -> list:
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        separators=["\n\n", "\n", ".", "!", "?"]
    )
    return splitter.split_text(text)

# ---------------------------
# Reranker using Ollama
# ---------------------------
def rerank_documents(query: str, docs: list, base_url: str, reranker_model: str = "dengcao/Qwen3-Reranker-8B:F16", top_k: int = 5) -> list:
    """
    Rerank documents using Ollama's reranker model
    Returns list of documents sorted by relevance
    """
    
    reranked_docs = []
    
    for i, doc in enumerate(docs):
        # Create a prompt for the reranker to score relevance
        prompt = f"""Score the relevance of the following document to the query on a scale of 0-10 (10 being most relevant).
Return only the numerical score.

Query: {query}

Document: {doc.page_content[:1000]}...

Score:"""
        
        try:
            response = requests.post(
                f"{base_url}/api/generate",
                json={
                    "model": reranker_model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0,
                        "num_predict": 10
                    }
                },
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                score_text = result.get('response', '0').strip()
                # Extract numerical score
                try:
                    score = float(score_text.split()[0])  # Get first number
                    score = max(0, min(10, score))  # Clamp between 0-10
                except:
                    score = 5.0  # Default score if parsing fails
                
                reranked_docs.append((doc, score))
            else:
                reranked_docs.append((doc, 5.0))
                
        except Exception as e:
            reranked_docs.append((doc, 5.0))
    
    # Sort by score (highest first) and return top_k
    reranked_docs.sort(key=lambda x: x[1], reverse=True)
    
    # Return just the documents in reranked order
    return [doc for doc, score in reranked_docs[:top_k]]

# ---------------------------
# Build vectorstore on remote Chroma
# ---------------------------
def build_vectorstore_remote_chroma(chunks: list) -> Chroma:
    # Configure Ollama embeddings for remote server - using your actual model
    embeddings = OllamaEmbeddings(
        model="mxbai-embed-large",
        base_url="http://192.168.1.253:11434"
    )
    
    # Use HttpClient for newer Chroma versions
    client = chromadb.HttpClient(
        host="192.168.1.253",
        port=8000
    )
    
    # Test connection
    try:
        heartbeat = client.heartbeat()
        print(f"✓ Connected to Chroma: {heartbeat}")
    except Exception as e:
        print(f"✗ Chroma connection failed: {e}")
        raise
    
    # Clean up existing collection if it exists
    try:
        client.delete_collection("apple_10k_collection")
        print("✓ Deleted existing collection")
    except Exception:
        print("✓ No existing collection to delete")
    
    vectorstore = Chroma.from_texts(
        texts=chunks,
        embedding=embeddings,
        client=client,
        collection_name="apple_10k_collection"
    )
    return vectorstore

# ---------------------------
# Format with LLM (optional)
# ---------------------------
def format_with_llm(query: str, docs: list, as_table: bool = False) -> str:
    # Use the new OllamaLLM class
    llm = OllamaLLM(
        model="qwen2.5:7b-instruct",
        temperature=0,
        base_url="http://192.168.1.253:11434"
    )
    
    context = "\n\n".join([doc.page_content for doc in docs])
    
    if as_table:
        instruction = f"""Based on the following Apple financial data, create clean, compact markdown tables for: {query}

Context:
{context}

Requirements:
1. Create 2-3 separate, narrow tables instead of one wide table
2. Keep column headers short (max 15 characters each)
3. Use abbreviations: "Rev" for Revenue, "Chg" for Change, "%" for percentage
4. Round numbers to whole millions (e.g., $265,595M or $265.6B)
5. Group related data: 
   - Table 1: Net Sales by Geographic Segment
   - Table 2: Net Sales by Product Category  
   - Table 3: Unit Sales - INCLUDE ALL THREE: iPhone, iPad, AND Mac unit sales
6. Maximum 4 columns per table
7. For unit sales, extract data for iPhone (217,722), iPad (43,535), AND Mac (18,209)
8. No explanatory text - just clean tables

Format as multiple compact markdown tables. Make sure to include Mac unit sales data."""
    else:
        instruction = f"Based on the following context, answer the query: {query}\n\nContext:\n{context}\n\nProvide a clear, direct response."
    
    try:
        result = llm.invoke(instruction)
        
        # Clean up the response - remove markdown code blocks and think tags
        cleaned = re.sub(r'```markdown\s*', '', result)
        cleaned = re.sub(r'```', '', cleaned)
        cleaned = re.sub(r'<think>.*?</think>', '', cleaned, flags=re.DOTALL)
        cleaned = cleaned.strip()
        
        return cleaned
        
    except Exception as e:
        print(f"✗ LLM request failed: {e}")
        return f"LLM formatting failed. Raw results:\n\n{context}"

# ---------------------------
# Main
# ---------------------------
def main():
    if len(sys.argv) < 3:
        print("Usage: python test_rag_rerank.py <pdf_file> <query> [<format>] [--no-rerank]")
        print("  format: 'table' for table formatting")
        print("  --no-rerank: Skip reranking step")
        sys.exit(1)
    
    pdf_file = sys.argv[1]
    query = sys.argv[2]
    format_option = sys.argv[3] if len(sys.argv) > 3 and not sys.argv[3].startswith('--') else None
    
    # Check for reranking option
    use_reranking = '--no-rerank' not in sys.argv
    as_table = format_option and "table" in format_option.lower()
    
    pdf_path = Path(pdf_file)
    if not pdf_path.exists():
        print(f"PDF file {pdf_file} not found!")
        sys.exit(1)
    
    print("=== Extracting text from PDF ===")
    text = extract_text_from_pdf(pdf_file)
    if not text:
        print("No text extracted from PDF!")
        sys.exit(1)
    
    print(f"✓ Extracted {len(text)} characters from PDF")
    
    print("=== Chunking text ===")
    chunks = smart_chunk_text(text)
    print(f"✓ Created {len(chunks)} chunks")
    
    print("=== Building vectorstore on remote Chroma ===")
    try:
        vectorstore = build_vectorstore_remote_chroma(chunks)
        print("✓ Vectorstore created successfully")
    except Exception as e:
        print(f"✗ Failed to create vectorstore: {e}")
        sys.exit(1)
    
    print(f"=== Searching for: '{query}' ===")
    try:
        # Get more documents initially for reranking
        initial_k = 10 if use_reranking else 5
        retrieved_docs = vectorstore.similarity_search(query, k=initial_k)
        print(f"✓ Retrieved {len(retrieved_docs)} initial documents")
    except Exception as e:
        print(f"✗ Search failed: {e}")
        sys.exit(1)
    
    # Rerank if requested
    if use_reranking:
        try:
            retrieved_docs = rerank_documents(
                query=query,
                docs=retrieved_docs,
                base_url="http://192.168.1.253:11434",
                reranker_model="dengcao/Qwen3-Reranker-8B:F16",
                top_k=5
            )
        except Exception as e:
            print(f"⚠️  Reranking failed ({e}), using original ranking")
    
    if as_table:
        result = format_with_llm(query, retrieved_docs, as_table=True)
    else:
        result = "\n\n".join([doc.page_content for doc in retrieved_docs])
    
    print("\n=== Retrieved Results ===\n")
    print(result)

if __name__ == "__main__":
    main()