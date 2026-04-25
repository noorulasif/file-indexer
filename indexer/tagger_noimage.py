"""
LLM Tagger module for the File Indexer.
Uses llama-cpp-python to generate metadata from extracted text.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

# Try to import llama-cpp-python
try:
    from llama_cpp import Llama
except ImportError:
    Llama = None
    print("Warning: llama-cpp-python not installed. Install with: pip install llama-cpp-python")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LLMTagger:
    """
    LLM-based tagger that generates metadata for text files.
    Uses a local GGUF model for inference.
    """
    
    # Default JSON schema for the output
    DEFAULT_RESPONSE = {
        "summary": "Could not parse LLM response",
        "document_type": "other",
        "tags": [],
        "keywords": [],
        "date_hint": None,
        "people_mentioned": []
    }
    
    def __init__(self, model_path: str, use_gpu: bool = False, gpu_layers: int = 0):
        """
        Initialize the LLM tagger.
        
        Args:
            model_path: Path to the .gguf model file
            use_gpu: Whether to use GPU acceleration
            gpu_layers: Number of layers to offload to GPU (0 = CPU only)
        """
        self.model_path = Path(model_path)
        self.use_gpu = use_gpu
        self.gpu_layers = gpu_layers if use_gpu else 0
        self.model = None
        self._load_model()
    
    def _load_model(self) -> None:
        """
        Load the LLM model using llama-cpp-python.
        """
        if Llama is None:
            logger.error("llama-cpp-python is not installed")
            return
        
        if not self.model_path.exists():
            logger.error(f"Model file not found: {self.model_path}")
            return
        
        try:
            logger.info(f"Loading model from {self.model_path}")
            logger.info(f"GPU: {self.use_gpu}, Layers: {self.gpu_layers}")
            
            self.model = Llama(
                model_path=str(self.model_path),
                n_ctx=2048,  # Context window size
                n_threads=4,  # Number of CPU threads
                n_gpu_layers=self.gpu_layers,
                verbose=False,  # Reduce console output
                use_mlock=False,  # Avoid memory locking issues
                use_mmap=True,  # Memory map the model for faster loading
            )
            logger.info("Model loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            self.model = None
    
    def is_loaded(self) -> bool:
        """
        Check if the model is loaded and ready.
        
        Returns:
            True if model is loaded, False otherwise
        """
        return self.model is not None
    
    def _build_prompt(self, file_path: str, extracted_text: str) -> str:
        """
        Build the prompt for the LLM.
        
        Args:
            file_path: Path to the file (used for context)
            extracted_text: Extracted text content
            
        Returns:
            Prompt string for the LLM
        """
        # Truncate text if it's too long (model has n_ctx=2048 tokens)
        # Rough estimate: 1 token ≈ 4 characters for English
        max_chars = 4000  # Safe limit for 2048 tokens
        if len(extracted_text) > max_chars:
            extracted_text = extracted_text[:max_chars] + "\n...[truncated]"
            logger.debug(f"Truncated text to {max_chars} characters")
        
        # Get filename for context
        filename = Path(file_path).name
        
        prompt = f"""You are a document metadata generator. Analyze the following file content and return ONLY a valid JSON object with the specified fields.

Filename: {filename}

File content: {extracted_text} text

Required JSON format:
{{
  "summary": "One or two sentence plain English description (max 200 chars)",
  "document_type": "One of: invoice|resume|passport|certificate|photo|report|contract|notes|spreadsheet|code|personal|other",
  "tags": ["tag1", "tag2", "tag3", "tag4", "tag5", "tag6"],
  "keywords": ["word1", "word2", "word3", "word4", "word5", "word6", "word7", "word8", "word9", "word10", "word11", "word12"],
  "date_hint": "YYYY or YYYY-MM or null",
  "people_mentioned": ["Person Name 1", "Person Name 2"]
}}

Rules:
1. Return ONLY raw JSON - no markdown code blocks, no explanation, no extra text
2. Summary must be concise (max 200 characters)
3. Tags: 3-8 short descriptive tags
4. Keywords: 5-12 important words/phrases from the content
5. Date hint: Use YYYY (e.g., "2024") or YYYY-MM (e.g., "2024-03") format, or null if no date found
6. People mentioned: Extract full names when possible, use empty list if none
7. For document_type, choose the best fit from the list provided

JSON:"""
        
        return prompt
    
    def _parse_response(self, response_text: str, file_path: str) -> Dict[str, Any]:
        """
        Parse the LLM's response and extract JSON.
        
        Args:
            response_text: Raw text from LLM
            file_path: Path to file (for error context)
            
        Returns:
            Parsed JSON dictionary
        """
        # Clean the response - remove any markdown code blocks
        cleaned = response_text.strip()
        
        # Remove markdown code blocks if present
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:]  # Remove ```json
        elif cleaned.startswith("```"):
            cleaned = cleaned[3:]  # Remove ```
        
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]  # Remove closing ```
        
        cleaned = cleaned.strip()
        
        # Try to find JSON object in the response (in case there's extra text)
        start_idx = cleaned.find("{")
        end_idx = cleaned.rfind("}") + 1
        
        if start_idx != -1 and end_idx > start_idx:
            json_str = cleaned[start_idx:end_idx]
        else:
            json_str = cleaned
        
        # Parse JSON
        try:
            result = json.loads(json_str)
            
            # Validate and ensure all required fields exist
            validated_result = self.DEFAULT_RESPONSE.copy()
            validated_result.update(result)
            
            # Ensure specific field types
            if not isinstance(validated_result["summary"], str):
                validated_result["summary"] = str(validated_result["summary"])[:200]
            
            if not isinstance(validated_result["document_type"], str):
                validated_result["document_type"] = "other"
            
            if not isinstance(validated_result["tags"], list):
                validated_result["tags"] = []
            
            if not isinstance(validated_result["keywords"], list):
                validated_result["keywords"] = []
            
            # Convert date_hint to string or None
            if validated_result["date_hint"] is not None:
                validated_result["date_hint"] = str(validated_result["date_hint"])
            
            if not isinstance(validated_result["people_mentioned"], list):
                validated_result["people_mentioned"] = []
            
            # Add filename as a keyword if keywords list is empty
            if not validated_result["keywords"]:
                validated_result["keywords"] = [Path(file_path).stem]
            
            logger.debug(f"Successfully parsed response: {validated_result}")
            return validated_result
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            logger.debug(f"Raw response: {response_text}")
            
            # Return fallback with filename
            fallback = self.DEFAULT_RESPONSE.copy()
            filename = Path(file_path).stem
            fallback["summary"] = f"Unable to process: {filename}"
            fallback["keywords"] = [filename]
            return fallback
    
    def tag_text_file(self, file_path: str, extracted_text: str) -> Dict[str, Any]:
        """
        Generate metadata for a text file using the LLM.
        
        Args:
            file_path: Path to the file (used for context)
            extracted_text: Extracted text content from the file
            
        Returns:
            Dictionary containing metadata (summary, document_type, tags, 
            keywords, date_hint, people_mentioned)
        """
        if not self.is_loaded():
            logger.error("Model not loaded, cannot tag file")
            fallback = self.DEFAULT_RESPONSE.copy()
            fallback["summary"] = "Model not available"
            return fallback
        
        if not extracted_text or extracted_text == "IMAGE_FILE":
            logger.warning(f"Empty or image file content for {file_path}")
            fallback = self.DEFAULT_RESPONSE.copy()
            fallback["summary"] = "No text content available"
            filename = Path(file_path).stem
            fallback["keywords"] = [filename]
            return fallback
        
        try:
            # Build prompt
            prompt = self._build_prompt(file_path, extracted_text)
            
            # Generate response from LLM
            logger.info(f"Tagging file: {Path(file_path).name}")
            
            response = self.model(
                prompt=prompt,
                max_tokens=512,  # Enough for JSON output
                temperature=0.1,  # Low temperature for consistent output
                top_p=0.95,
                stop=["```", "}",  # Stop tokens - we'll add back the closing brace
                echo=False,
                frequency_penalty=0.0,
                presence_penalty=0.0
            )
            
            # Extract the generated text
            generated_text = response['choices'][0]['text'].strip()
            
            # Add back closing brace if it was cut off
            if generated_text and not generated_text.endswith('}'):
                # Count opening braces
                open_braces = generated_text.count('{')
                close_braces = generated_text.count('}')
                if open_braces > close_braces:
                    generated_text += '}' * (open_braces - close_braces)
            
            logger.debug(f"Generated response: {generated_text[:200]}...")
            
            # Parse the response
            result = self._parse_response(generated_text, file_path)
            
            # Add file metadata
            result["file_name"] = Path(file_path).name
            result["file_path"] = file_path
            
            logger.info(f"Successfully tagged {Path(file_path).name}: "
                       f"Type={result['document_type']}, "
                       f"Tags={len(result['tags'])}, "
                       f"Keywords={len(result['keywords'])}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error tagging file {file_path}: {e}", exc_info=True)
            fallback = self.DEFAULT_RESPONSE.copy()
            fallback["summary"] = f"Error during processing: {str(e)[:100]}"
            filename = Path(file_path).stem
            fallback["keywords"] = [filename]
            return fallback
    
    def get_model_info(self) -> Dict[str, Any]:
        """
        Get information about the loaded model.
        
        Returns:
            Dictionary with model information
        """
        if not self.is_loaded():
            return {"loaded": False}
        
        return {
            "loaded": True,
            "model_path": str(self.model_path),
            "use_gpu": self.use_gpu,
            "gpu_layers": self.gpu_layers,
            "context_size": 2048
        }


def main():
    """Test the LLM tagger with a sample text."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test LLM tagger")
    parser.add_argument("model_path", help="Path to GGUF model file")
    parser.add_argument("--file", "-f", help="Path to file to tag (optional)")
    parser.add_argument("--text", "-t", help="Direct text to tag (optional)")
    parser.add_argument("--gpu", action="store_true", help="Use GPU acceleration")
    parser.add_argument("--gpu-layers", type=int, default=0, help="Number of GPU layers")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Sample text if no file or text provided
    if not args.file and not args.text:
        sample_text = """
        INVOICE
        Date: 2024-03-15
        Invoice #: INV-2024-001
        Bill To: John Smith, 123 Main St, Anytown, USA
        Items:
        - 2x Laptop Stand @ $45.00 = $90.00
        - 1x Wireless Mouse @ $25.00 = $25.00
        Subtotal: $115.00
        Tax (10%): $11.50
        Total: $126.50
        Thank you for your business!
        """
        print("Using sample invoice text...")
        text_to_tag = sample_text
        file_path = "sample_invoice.txt"
    elif args.file:
        # Read from file
        file_path = args.file
        with open(file_path, 'r', encoding='utf-8') as f:
            text_to_tag = f.read()
        print(f"Loaded file: {file_path}")
    else:
        # Use direct text
        file_path = "direct_input.txt"
        text_to_tag = args.text
        print("Using direct text input...")
    
    # Initialize tagger
    print(f"\nLoading model from: {args.model_path}")
    print(f"GPU: {args.gpu}, GPU Layers: {args.gpu_layers}")
    
    tagger = LLMTagger(
        model_path=args.model_path,
        use_gpu=args.gpu,
        gpu_layers=args.gpu_layers
    )
    
    if not tagger.is_loaded():
        print("❌ Failed to load model")
        sys.exit(1)
    
    print("✅ Model loaded successfully")
    print("\n" + "=" * 60)
    print("TAGGING FILE...")
    print("=" * 60)
    
    # Tag the text
    result = tagger.tag_text_file(file_path, text_to_tag)
    
    # Display results
    print("\n📋 METADATA RESULTS:")
    print("-" * 60)
    print(f"Summary: {result['summary']}")
    print(f"Document Type: {result['document_type']}")
    print(f"Tags: {', '.join(result['tags'])}")
    print(f"Keywords: {', '.join(result['keywords'][:10])}")  # Show first 10
    print(f"Date Hint: {result['date_hint']}")
    print(f"People Mentioned: {', '.join(result['people_mentioned']) if result['people_mentioned'] else 'None'}")
    print("-" * 60)
    
    # Pretty print full JSON
    print("\n📄 FULL JSON OUTPUT:")
    output_json = {k: v for k, v in result.items() if k not in ['file_path', 'file_name']}
    print(json.dumps(output_json, indent=2, ensure_ascii=False))
    
    # Show model info
    print("\n🔧 MODEL INFO:")
    info = tagger.get_model_info()
    for key, value in info.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()


