"""Turn an Excel-exported SoS results page into a list of rows of cells, keeping
blank cells and expanding colspans so a value stays under its candidate's column."""
import re,html
def rows(path):
    t=open(path,errors='ignore').read()
    t=re.sub(r'<(style|script)[^>]*>.*?</\1>','',t,flags=re.S|re.I)
    # FrontPage wraps some header cells in a one-cell nested table: unwrap it in place
    t=re.sub(r'<table[^>]*>\s*<tr[^>]*>\s*<td[^>]*>(.*?)</td>\s*</tr>\s*</table>',r'\1',t,flags=re.S|re.I)
    out=[]
    # split on <tr> openings, not <tr>..</tr> pairs: some exported rows are never closed
    for tr in re.split(r'<tr\b[^>]*>',t,flags=re.I)[1:]:
        tr=re.split(r'</table>',tr,flags=re.I)[0]  # last row of a table
        cells=[]
        for m in re.finditer(r'<t([dh])\b([^>]*)>(.*?)(?=</t[dh]>|<t[dh]\b|$)',tr,re.S|re.I):
            a,c=m.group(2),m.group(3)
            s=re.sub(r'\s+',' ',html.unescape(re.sub(r'<[^>]+>',' ',c)).replace('\xa0',' ')).strip()
            sp=re.search(r'colspan\s*=\s*["\']?(\d+)',a,re.I)
            m2=re.match(r'^(\d[\d,]*)\s+(\d[\d,]*)$',s)
            if m2: s=m2.group(2)        # "3877 3881": first count, then recount - the recount stands
            s=re.sub(r'\s+Recount$','',s) if re.search(r',\s*\w+\s+Recount$',s) else s   # "Bridle, r Recount"
            s=re.sub(r'\bRecoun\s+t\b','Recount',s)                     # 'Recoun t' typo
            if re.match(r'^RECOUNT\s*/\s*BLC$',s,re.I): s='Recount'          # recount column, BLC-adjusted
            m3=re.match(r'^(.*?,\s*[a-z&]+)\s+Recount$',s)
            if m3: s=m3.group(1)                                             # 'Salatiello, d Recount' (the recount column is the next cell)
            s=re.sub(r'(,\s*[a-z]{1,3}),$',r'\1',s)                          # 'Lynde, d,'
            cells.append(s)
            if sp: cells+=['']*(int(sp.group(1))-1)
        while cells and cells[-1]=='': cells.pop()
        if any(cells): out.append(cells)
    return out
