import pandas as pd
import re
import os
import logging
import html

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

INPUT_FILE = "Downloaddatanotes_with_notes.csv"
OUTPUT_FILE = "Downloaddatanotes_cleaned.csv"

def clean_notes():
    if not os.path.exists(INPUT_FILE):
        logger.error(f"Input file '{INPUT_FILE}' not found.")
        return

    try:
        logger.info(f"Reading {INPUT_FILE}...")
        df = pd.read_csv(INPUT_FILE)
        
        # Identify note columns
        note_columns = [col for col in df.columns if col.startswith('note') and col[4:].isdigit()]
        logger.info(f"Found {len(note_columns)} note columns: {note_columns}")
        
        if not note_columns:
            logger.warning("No note columns found to clean.")
            return

        cleaned_rows = []
        
        for index, row in df.iterrows():
            # Extract notes from the row
            current_notes = []
            for col in note_columns:
                val = row[col]
                if pd.notna(val):
                    current_notes.append(str(val))
            
            valid_notes = []
            for note in current_notes:
                # 1. Check if note contains [ WAVV: ... ] pattern - if so, skip entire note
                if re.search(r'\[\s*WAVV:\s*[a-f0-9-]+\s*\]', note):
                    continue

                # 2. Clean HTML content
                # Replace common block tags with newlines to preserve structure
                cleaned_note = re.sub(r'<(br|p|div)[^>]*>', '\n', note, flags=re.IGNORECASE)
                # Strip all other HTML tags
                cleaned_note = re.sub(r'<[^>]+>', '', cleaned_note)
                # Unescape HTML entities (e.g. &amp; -> &)
                cleaned_note = html.unescape(cleaned_note)
                
                cleaned_note = cleaned_note.strip()

                # 3. Check if empty or starts with "WAVV Call"
                if cleaned_note and not cleaned_note.startswith("WAVV Call"):
                    valid_notes.append(cleaned_note)
            
            # Update row: Clear old note columns first
            for col in note_columns:
                row[col] = None
            
            # Fill with valid notes
            for i, valid_note in enumerate(valid_notes):
                # If we have more valid notes than existing columns, we might need to handle that
                # But since we are cleaning, we usually have fewer or equal notes.
                # However, if the original file had gaps (unlikely from previous script), we are fine.
                # If we somehow have more notes than columns (impossible if we just extracted them), 
                # we fit what we can.
                col_name = f"note{i+1}"
                if col_name in df.columns:
                    row[col_name] = valid_note
                else:
                    # If the column doesn't exist (e.g. we compacted notes but maybe logic implies we stay within bounds),
                    # actually we should be fine since we are filtering down.
                    # But if we want to be safe, we could add columns, but let's stick to existing structure for now
                    # or just add it to the row dict if we were building a list of dicts.
                    # Since we are modifying 'row' which is a Series, adding a new index might not work well if we assign back to df.
                    # Better approach: Build a list of dicts.
                    pass
            
            cleaned_rows.append(row)

        # Create new DataFrame from cleaned rows
        cleaned_df = pd.DataFrame(cleaned_rows)
        
        # Save
        cleaned_df.to_csv(OUTPUT_FILE, index=False)
        logger.info(f"Successfully cleaned notes and saved to {OUTPUT_FILE}")

    except Exception as e:
        logger.error(f"Error cleaning notes: {str(e)}")

if __name__ == "__main__":
    clean_notes()
