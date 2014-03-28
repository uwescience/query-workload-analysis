import sqlparse


def get_tokens(q):
    q = q.replace('[','"').replace(']','"').lower().replace('union all',';').strip()
    if q[0] == '(':
        q = q.strip('(').strip(')')
    tokens = []
    parsed = sqlparse.parse(q)
    for item in parsed:
        for t in item.tokens:
            if t.value == ',' or t.value == ';':
                continue
            if str(t.ttype) == "None":
                if str(t)[0] == '(':
                    for token in get_tokens(str(t)):
                        tokens.append(token)
            elif str(t.ttype) == "Token.Text.Whitespace" or str(t.ttype) == 'Token.Wildcard':
                continue
            else:
                tokens.append(str(t))
    return tokens
