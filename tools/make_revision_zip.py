import os
import zipfile

out_dir = os.path.join('outputs', date_dir := '2025-12-13')
project_tag = 'J.M_Stock_Project_2025-12-13'
files = [
    f'top_symbols_{project_tag}.csv',
    f'top_symbols_{project_tag}.xlsx',
    f'top_symbols_{project_tag}.pdf',
    f'removed_symbols_{project_tag}.csv',
    f'filters_summary_{project_tag}.json',
    f'metadata_{project_tag}.json'
]

paths = [os.path.join(out_dir, f) for f in files if os.path.exists(os.path.join(out_dir, f))]
if not paths:
    print('No matching files found in', out_dir)
    raise SystemExit(1)

zip_name = os.path.join(out_dir, 'JM_Stock_Project_revision_01.zip')
with zipfile.ZipFile(zip_name, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
    for p in paths:
        zf.write(p, arcname=os.path.basename(p))

print('Created:', zip_name)
print('Contents:')
with zipfile.ZipFile(zip_name, 'r') as zf:
    for info in zf.infolist():
        print(f" - {info.filename} ({info.file_size} bytes)")
