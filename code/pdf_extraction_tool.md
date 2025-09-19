# 📄 PDF Extraction + Diagnostics Tool

This Python script extracts text from PDF files, tests the Ollama LLM connection, and falls back to OCR when necessary. It is designed to integrate into larger projects for automated PDF processing and diagnostics.

---

## 🔑 Key Features

1. **Ollama Connection Test**
    - Checks if Ollama API is reachable.
    - Lists available models.
    - Prints detailed diagnostics.

2. **PDF Text Extraction**
    - Uses **PyMuPDF (fitz)** to extract text directly.
    - If no text is detected, optionally uses **Tesseract OCR** as fallback.
    - Supports multi-page PDFs.

3. **Output**
    - Saves extracted text to a `.txt` file named after the PDF.
    - Prints page-level extraction info and diagnostics.

---

## 🛠️ Dependencies

- `fitz` (PyMuPDF)
- `pdfplumber` (optional)
- `requests` (for Ollama API)
- `pytesseract` + `Pillow` (optional, for OCR fallback)
- Python standard libraries: `os`, `pathlib`, `io`

**Note:** For Windows, the script attempts to auto-detect Tesseract installation paths.

---

## 🧩 Core Functions

### 1. `test_ollama_connection(host)`
- Connects to Ollama API and checks availability.
- Returns `success` status, host URL, and available models.

### 2. `extract_pdf_text(pdf_path)`
- Iterates through each PDF page:
    - Extracts text with `fitz`.
    - If no text is found and OCR is available:
        - Converts page to image.
        - Runs Tesseract OCR to extract text.
- Returns combined text of all pages.

---

## 🚀 Example Workflow

```python
# Test Ollama connection
success, host, models = test_ollama_connection()

# Extract text from a PDF
pdf_text = extract_pdf_text("my_document.pdf")

# Save extracted text
with open("my_document_extracted.txt", "w", encoding="utf-8") as f:
    f.write(pdf_text)
```

**Output:**
```
🔍 OLLAMA CONNECTION DIAGNOSTICS
✅ Connection successful! Status: 200
📋 Available models: ['SimonPu/Qwen3-Coder:30B-Instruct_Q4_K_XL']

📚 PDF EXTRACTION & OCR
✅ Page 1: extracted 1024 chars (text)
🔍 Page 2: OCR extracted 850 chars
✅ Extracted text saved to: my_document_extracted.txt
```

---

## 🔗 Integration into a Larger Project

1. **Module Structure**
```
project/
│
├─ pdf_tool/
│   ├─ __init__.py
│   ├─ pdf_extractor.py  # contains this script
│   └─ utils.py
│
├─ main_app.py           # imports pdf_extractor
├─ requirements.txt
└─ ...
```

2. **Usage in Project**
```python
from pdf_tool.pdf_extractor import extract_pdf_text, test_ollama_connection

# Test Ollama API
success, host, models = test_ollama_connection()

# Extract PDF content
text = extract_pdf_text("path/to/file.pdf")
print(text[:500])  # preview first 500 characters
```

3. **Automation**
- Can be used in ETL pipelines to process large batches of PDFs.
- Useful in projects where PDFs contain code, legal documents, or reports.
- Can feed extracted text into an LLM (like Ollama) for further processing, summarization, or code conversion.

---

## ✅ Summary
- Tests and reports Ollama connection.
- Extracts PDF text with hybrid method (text first, OCR fallback).
- Saves results as `.txt`.
- Designed for modular integration into larger Python projects.

