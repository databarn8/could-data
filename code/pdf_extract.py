"""
PDF Extraction + Diagnostics Tool
- Tests Ollama connection
- Extracts text using pdfplumber/fitz
- Falls back to OCR when needed
- Saves results into a .txt file
"""

import requests
import fitz  # PyMuPDF
import pdfplumber
from pathlib import Path
import os
import io

# Configure Tesseract path for Windows
try:
    import pytesseract
    from PIL import Image
    PYTESSERACT_AVAILABLE = True
    
    if os.name == 'nt':  # Windows
        possible_paths = [
            r'C:\Program Files\Tesseract-OCR\tesseract.exe',
            r'C:\Program Files (x86)\Tesseract-OCR\tesseract.exe',
            r'C:\Users\%USERNAME%\AppData\Local\Programs\Tesseract-OCR\tesseract.exe'
        ]
        
        for path in possible_paths:
            expanded_path = path.replace('%USERNAME%', os.getenv('USERNAME', ''))
            if os.path.exists(expanded_path):
                pytesseract.pytesseract.tesseract_cmd = expanded_path
                print(f"🔧 Found Tesseract at: {expanded_path}")
                break
        else:
            print("⚠️  Tesseract path not auto-detected, using default")
except ImportError:
    PYTESSERACT_AVAILABLE = False
    print("❌ pytesseract or Pillow not installed")

def test_ollama_connection(host="192.168.1.253:11434"):
    """Test Ollama connection with detailed diagnostics"""
    print("\n🔍 OLLAMA CONNECTION DIAGNOSTICS")
    print("=" * 50)
    
    if not host.startswith(('http://', 'https://')):
        host = f"http://{host}"
    print(f"🔗 Testing connection to: {host}")
    
    try:
        response = requests.get(f"{host}/api/tags", timeout=10)
        print(f"✅ Connection successful! Status: {response.status_code}")
        if response.status_code == 200:
            models = response.json()
            available_models = [m['name'] for m in models.get('models', [])]
            print(f"📋 Available models: {available_models}")
            return True, host, available_models
    except Exception as e:
        print(f"❌ Ollama connection failed: {e}")
    return False, host, []

def extract_pdf_text(pdf_path):
    """Extract text from a PDF using hybrid method (text first, OCR fallback)"""
    print("\n📚 PDF EXTRACTION & OCR")
    print("=" * 50)

    pdf_file = Path(pdf_path)
    if not pdf_file.exists():
        print(f"❌ PDF not found: {pdf_file}")
        return ""
    
    doc = fitz.open(pdf_path)
    all_text = []

    for page_num in range(doc.page_count):
        page = doc[page_num]
        text = page.get_text().strip()

        if text:
            all_text.append(f"\n--- Page {page_num+1} (text) ---\n{text}")
            print(f"✅ Page {page_num+1}: extracted {len(text)} chars (text)")
        elif PYTESSERACT_AVAILABLE:
            # OCR fallback
            mat = fitz.Matrix(2, 2)  # 2x zoom
            pix = page.get_pixmap(matrix=mat)
            img = Image.open(io.BytesIO(pix.tobytes("png")))
            ocr_text = pytesseract.image_to_string(img, config="--oem 3 --psm 6").strip()
            if ocr_text:
                all_text.append(f"\n--- Page {page_num+1} (OCR) ---\n{ocr_text}")
                print(f"🔍 Page {page_num+1}: OCR extracted {len(ocr_text)} chars")
            else:
                print(f"⚠️ Page {page_num+1}: no text found, OCR failed")
        else:
            print(f"⚠️ Page {page_num+1}: no text and OCR unavailable")

    doc.close()
    return "\n".join(all_text)

def main():
    print("🚀 PDF PROCESSING TOOL")
    print("=" * 50)

    # Test Ollama connection
    success, host, models = test_ollama_connection()

    # Choose a PDF file
    pdf_path = "constitution.pdf"  # change to your test file
    extracted_text = extract_pdf_text(pdf_path)

    if extracted_text:
        out_file = Path(pdf_path).stem + "_extracted.txt"
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(extracted_text)
        print(f"\n✅ Extracted text saved to: {out_file}")
    else:
        print("\n❌ No text extracted from PDF")

    print("\n📋 SUMMARY")
    print("=" * 50)
    if success:
        print(f"✅ Ollama connection working ({host})")
        print(f"   Models: {models}")
    else:
        print("❌ Ollama connection failed")

if __name__ == "__main__":
    main()
