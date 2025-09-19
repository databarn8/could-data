# 📸 OCR to Ollama Python Code Generator

This script takes a **screenshot (image file containing code)**, extracts text using **OCR (EasyOCR + Tesseract)**, and then asks **Ollama** (local LLM API) to clean up the OCR output into real Python code.  

---

## 🔑 Key Components

### 1. **Imports**
- `easyocr`, `pytesseract`: OCR libraries  
- `cv2`, `numpy`, `PIL`: Image preprocessing  
- `requests`: Talk to Ollama API  
- `sys`, `os`: Handle file input/output  

---

### 2. **OllamaClient**
- Connects to Ollama running locally (`http://192.168.1.253:11434`)  
- Uses the model `SimonPu/Qwen3-Coder:30B-Instruct_Q4_K_XL`  
- Sends a **prompt** and returns generated text  

```python
client = OllamaClient()
response = client.generate("some text")
```

---

### 3. **CodeScreenshotOCR**
Handles **image preprocessing and OCR**:  

- **`preprocess_for_code`**  
  - Enhance contrast & sharpness  
  - Resize small images  
  - Convert to grayscale, denoise, threshold  

- **`extract_with_easyocr`**  
  - Runs EasyOCR  
  - Keeps text with confidence > 0.2  
  - Sorts results by line  

- **`extract_with_tesseract`**  
  - Runs Tesseract OCR with config (`--oem 3 --psm 4`)  
  - Falls back to grayscale if needed  

- **`extract_code`**  
  - Chooses EasyOCR, Tesseract, or both  
  - Returns results as dictionary  

---

### 4. **Main Workflow**
When you run:  
```bash
python ocr_to_ollama.py test.png
```

Steps:  
1. Load input image (`test.png`)  
2. Extract text with **EasyOCR & Tesseract**  
3. Print first 500 chars of results  
4. Save raw OCR outputs to:  
   - `test_easyocr.txt`  
   - `test_tesseract.txt`  
5. Send OCR text to Ollama → generate **clean Python code**  
6. Save LLM-generated code to:  
   - `test_easyocr.py`  
   - `test_tesseract.py`  
7. Compare line counts (EasyOCR vs Tesseract)  

---

## 🖼️ Example Run
```bash
python ocr_to_ollama.py myscript.png
```

Output:
```
Extracting code from screenshot: myscript.png
============================================================
Trying EasyOCR...
Trying Tesseract...

EASYOCR RESULTS:
----------------------------------------
print("Hello W0r1d!") ...

Saved raw OCR to myscript_easyocr.txt
✅ Ollama Python code saved to myscript_easyocr.py

TESSERACT RESULTS:
----------------------------------------
print("Hello World!") ...

Saved raw OCR to myscript_tesseract.txt
✅ Ollama Python code saved to myscript_tesseract.py

COMPARISON:
EasyOCR extracted 12 lines
Tesseract extracted 15 lines
```

---

## 🚀 Summary
- **OCR** extracts code text from screenshots  
- **Saves raw text** for debugging  
- **Ollama LLM** converts OCR text → clean Python code  
- **Outputs `.txt` + `.py` files** for both EasyOCR & Tesseract  
