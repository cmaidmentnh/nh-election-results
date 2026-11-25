"""Rows of cells from an SoS .xls/.xlsx results workbook (every sheet, in order), shaped like
grid.rows(): strings, blanks kept, trailing blanks dropped."""
import re
def _fmt(v):
    if v is None: return ''
    if isinstance(v,float): return str(int(v)) if v==int(v) else str(v)
    return re.sub(r'\s+',' ',str(v)).strip()
def rows(path):
    with open(path,'rb') as f: head=f.read(8)
    out=[]
    if head.startswith(b'PK'):
        import openpyxl,shutil,tempfile,os
        tmp=os.path.join(tempfile.mkdtemp(),'w.xlsx');shutil.copy(path,tmp)   # openpyxl wants the extension
        wb=openpyxl.load_workbook(tmp,data_only=True,read_only=True)
        sheets=[[[_fmt(c) for c in r] for r in ws.iter_rows(values_only=True)] for ws in wb.worksheets]
    else:
        import xlrd
        wb=xlrd.open_workbook(path)
        sheets=[[[_fmt(ws.cell_value(i,j)) for j in range(ws.ncols)] for i in range(ws.nrows)] for ws in wb.sheets()]
    for sh in sheets:
        for r in sh:
            while r and r[-1]=='': r.pop()
            if any(r): out.append(r)
    return out
