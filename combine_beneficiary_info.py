import os
import csv

input_dir = "output_beneficiary_info"
output_file = "combined_beneficiary_info.csv"

combined = []
for fname in os.listdir(input_dir):
    if fname.endswith(".csv"):
        account_id = fname.split("_")[-1].replace(".csv", "")
        with open(os.path.join(input_dir, fname), "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                contact_id = row.get("Contact ID")
                beneficiary = row.get("Beneficiary Information")
                if contact_id:
                    combined.append([contact_id, beneficiary, account_id])

with open(output_file, "w", newline='', encoding="utf-8") as out_f:
    writer = csv.writer(out_f)
    writer.writerow(["Contact ID", "Beneficiary Information", "Account Id"])
    writer.writerows(combined)

print(f"Combined sheet saved as {output_file}")
