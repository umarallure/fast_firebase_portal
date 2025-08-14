from together import Together
import csv
import random

client = Together(api_key="tgp_v1_Zobcx3SwLWn9batYRVklleB4zKa1KPiLe7sPgCHv7f0")

def generate_notes(beneficiary_info):
    prompt = (
        "Extract structured notes from the following beneficiary information. For each beneficiary, provide:\n"
        "Name: [name], Relationship: [relationship or N/A], DOB: [date or N/A], Phone Number: [phone or N/A]\n"
        "If there are multiple beneficiaries, list each on a new line.\n"
        "Output ONLY the structured notes in the template format, with NO extra text, explanation, or introduction.\n"
        f"Beneficiary Information: {beneficiary_info}"
    )
    response = client.chat.completions.create(
        model="google/gemma-3n-E4B-it",
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content.strip()
def generate_embedding(text):
    response = client.embeddings.create(
        model="togethercomputer/m2-bert-80M-32k-retrieval",
        input=text
    )
    return response.data[0].embedding


# Process all rows and write notes to a new CSV


# Process all entries and write notes to a new CSV, skipping rows without Beneficiary Information

batch_size = 100
batch_size = 100
input_file = "output_beneficiary_info/beneficiary_info_33.csv"
output_file = "output_beneficiary_info/beneficiary_info_33_with_notes.csv"

batch_size = 100
with open(input_file, "r", encoding="latin1") as f_in:
    reader = csv.DictReader(f_in)
    fieldnames = reader.fieldnames + ["Notes"]
    rows = [row for row in reader if row.get("Beneficiary Information", "")]

with open(output_file, "w", newline='', encoding="latin1") as f_out:
    writer = csv.DictWriter(f_out, fieldnames=fieldnames)
    writer.writeheader()
    total = len(rows)
    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        batch = rows[start:end]
        print(f"Processing batch {start//batch_size + 1}: contacts {start+1} to {end} of {total}")
        for idx, row in enumerate(batch, start=start+1):
            beneficiary_info = row["Beneficiary Information"]
            notes = generate_notes(beneficiary_info)
            row["Notes"] = notes
            writer.writerow(row)
            print(f"Processed contact {idx}: Notes generated.")
print(f"Notes column added for {total} contacts with Beneficiary Information. Output written to {output_file}.")
