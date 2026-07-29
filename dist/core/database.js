// Main database class: FlatBuffers-SQLite
// Provides SQL query interface over FlatBuffer storage
import { TableStore } from './table-store.js';
import { StackedFlatBufferStore } from '../storage/index.js';
import { parseSchema } from '../schema/index.js';
// Security: Maximum SQL query length to prevent ReDoS attacks
const MAX_SQL_LENGTH = 10000;
// Security: Maximum queries per second (0 = unlimited)
const DEFAULT_RATE_LIMIT = 0;
export class FlatSQLDatabase {
    schema;
    storage;
    tables = new Map();
    accessor;
    // Security: Configuration options
    maxSqlLength;
    rateLimit;
    queryTimestamps = [];
    constructor(schema, accessor, options = {}) {
        this.schema = schema;
        this.storage = new StackedFlatBufferStore(schema.name);
        this.accessor = accessor;
        this.maxSqlLength = options.maxSqlLength ?? MAX_SQL_LENGTH;
        this.rateLimit = options.rateLimit ?? DEFAULT_RATE_LIMIT;
        // Initialize table stores
        for (const tableDef of schema.tables) {
            const tableStore = new TableStore(tableDef, this.storage, (data, path) => this.accessor.getField(data, path));
            this.tables.set(tableDef.name, tableStore);
        }
    }
    // Create database from schema source (IDL or JSON Schema)
    static fromSchema(source, accessor, name = 'default', options = {}) {
        const schema = parseSchema(source, name);
        return new FlatSQLDatabase(schema, accessor, options);
    }
    // Insert a record (as JSON that will be converted to FlatBuffer)
    insert(tableName, data) {
        const tableStore = this.tables.get(tableName);
        if (!tableStore) {
            throw new Error(`Table not found: ${tableName}`);
        }
        const buffer = this.accessor.buildBuffer(tableName, data);
        return tableStore.insert(buffer);
    }
    // Insert raw FlatBuffer data
    insertRaw(tableName, flatbufferData) {
        const tableStore = this.tables.get(tableName);
        if (!tableStore) {
            throw new Error(`Table not found: ${tableName}`);
        }
        return tableStore.insert(flatbufferData);
    }
    // Stream in multiple FlatBuffers
    stream(tableName, flatbuffers) {
        const rowids = [];
        for (const fb of flatbuffers) {
            rowids.push(this.insertRaw(tableName, fb));
        }
        return rowids;
    }
    // Create a runtime index without requiring database-specific schema annotations.
    createIndex(tableName, columnName) {
        const tableStore = this.tables.get(tableName);
        if (!tableStore) {
            throw new Error(`Table not found: ${tableName}`);
        }
        tableStore.createIndex(columnName);
    }
    getStorageBytes() {
        return this.storage.getCurrentSize();
    }
    // Execute a simple SQL query.
    //
    // Supported grammar:
    //   SELECT <columns> FROM <table> [WHERE <expr>] [ORDER BY <col> [ASC|DESC], ...] [LIMIT <n>]
    //
    // <expr> supports AND / OR / NOT / parentheses over these predicates:
    //   col = != <> < > <= >= value
    //   col [NOT] BETWEEN low AND high
    //   col [NOT] IN (v1, v2, ...)
    //   col [NOT] LIKE 'pattern'      -- % and _ wildcards, ASCII case-insensitive (SQLite default)
    //   col IS [NOT] NULL
    //
    // FAIL-CLOSED CONTRACT: a WHERE or ORDER BY clause that this grammar cannot
    // parse throws. It must NEVER degrade into an unfiltered full-table scan --
    // silently returning every row to a caller that asked for a subset is a
    // wrong answer that no consumer can detect.
    query(sql) {
        // Security: Check SQL length to prevent ReDoS
        if (sql.length > this.maxSqlLength) {
            throw new Error(`SQL query exceeds maximum length of ${this.maxSqlLength} characters`);
        }
        // Security: Rate limiting
        if (this.rateLimit > 0) {
            this.enforceRateLimit();
        }
        const parsed = this.parseSQL(sql);
        if (parsed.type !== 'SELECT') {
            throw new Error(`Unsupported query type: ${parsed.type}`);
        }
        const tableStore = this.tables.get(parsed.table);
        if (!tableStore) {
            throw new Error(`Table not found: ${parsed.table}`);
        }
        let records = selectRecords(tableStore, parsed.where);
        if (parsed.orderBy !== undefined) {
            records = sortRecords(records, parsed.orderBy);
        }
        if (parsed.limit !== undefined) {
            records = records.slice(0, parsed.limit);
        }
        // Determine columns
        const tableDef = tableStore.getTableDef();
        const columns = parsed.columns[0] === '*'
            ? tableDef.columns.map(c => c.name)
            : parsed.columns;
        // Build result rows
        const rows = records.map(record => {
            return columns.map(col => {
                if (col === '_rowid')
                    return record.rowid;
                if (col === '_offset')
                    return record.offset;
                if (col === '_data')
                    return record.data;
                return record.fields.get(col) ?? null;
            });
        });
        return {
            columns,
            rows,
            rowCount: rows.length,
        };
    }
    parseSQL(sql) {
        const clauses = splitSelectStatement(sql.trim());
        if (!clauses) {
            // Security: Don't expose SQL in error message (could contain sensitive data)
            throw new Error('SQL parse error: unsupported query syntax');
        }
        const columns = splitTopLevel(clauses.columns, ',').map(c => c.trim());
        if (columns.length === 0 || columns.some(c => c.length === 0)) {
            throw new Error('SQL parse error: empty column in SELECT list');
        }
        if (!/^\w+$/.test(clauses.table)) {
            throw new Error('SQL parse error: unsupported FROM target');
        }
        let limit;
        if (clauses.limit !== undefined) {
            if (!/^\d+$/.test(clauses.limit.trim())) {
                // Fail closed: an unparsed LIMIT must not silently return every row.
                throw new Error('SQL parse error: LIMIT requires a non-negative integer');
            }
            limit = Number(clauses.limit.trim());
        }
        // Fail closed: parseWhereClause / parseOrderByClause throw on anything the
        // grammar does not understand. Neither may fall back to "no filter".
        const where = clauses.where === undefined ? undefined : parseWhereClause(clauses.where);
        const orderBy = clauses.orderBy === undefined
            ? undefined
            : parseOrderByClause(clauses.orderBy);
        return {
            type: 'SELECT',
            table: clauses.table,
            columns,
            where,
            orderBy,
            limit,
        };
    }
    // Security: Rate limiting enforcement
    enforceRateLimit() {
        const now = Date.now();
        const windowMs = 1000; // 1 second window
        // Remove timestamps older than the window
        this.queryTimestamps = this.queryTimestamps.filter(ts => now - ts < windowMs);
        if (this.queryTimestamps.length >= this.rateLimit) {
            throw new Error(`Rate limit exceeded: maximum ${this.rateLimit} queries per second`);
        }
        this.queryTimestamps.push(now);
    }
    // Get table definition
    getTableDef(tableName) {
        return this.tables.get(tableName)?.getTableDef();
    }
    // List all tables
    listTables() {
        return Array.from(this.tables.keys());
    }
    // Get raw storage data (for export)
    exportData() {
        return this.storage.getData();
    }
    // Get schema
    getSchema() {
        return this.schema;
    }
    // Get statistics
    getStats() {
        return Array.from(this.tables.entries()).map(([name, store]) => ({
            tableName: name,
            recordCount: store.getRecordCount(),
            indexes: store.getIndexNames(),
        }));
    }
}
/** Keywords that terminate the clause preceding them, in statement order. */
const SELECT_CLAUSE_KEYWORDS = ['FROM', 'WHERE', 'ORDER BY', 'LIMIT'];
/**
 * Walk `sql` and report every top-level (unquoted, paren-depth 0) occurrence of
 * a clause keyword. Returns undefined if the string has unbalanced quotes or
 * parentheses, which callers treat as a parse failure.
 */
function scanTopLevel(sql, onKeyword, onDelimiter, delimiters) {
    let depth = 0;
    let i = 0;
    while (i < sql.length) {
        const ch = sql[i];
        if (ch === "'" || ch === '"') {
            const quote = ch;
            i++;
            let closed = false;
            while (i < sql.length) {
                if (sql[i] === quote) {
                    // Doubled quote is an escaped quote, not a terminator.
                    if (sql[i + 1] === quote) {
                        i += 2;
                        continue;
                    }
                    i++;
                    closed = true;
                    break;
                }
                i++;
            }
            if (!closed) {
                return false;
            }
            continue;
        }
        if (ch === '(') {
            depth++;
            i++;
            continue;
        }
        if (ch === ')') {
            depth--;
            if (depth < 0) {
                return false;
            }
            i++;
            continue;
        }
        if (depth === 0 && delimiters && onDelimiter && delimiters.includes(ch)) {
            onDelimiter(ch, i);
            i++;
            continue;
        }
        if (depth === 0 && onKeyword && isWordBoundary(sql, i)) {
            let matched;
            for (const keyword of SELECT_CLAUSE_KEYWORDS) {
                const end = matchKeywordAt(sql, i, keyword);
                if (end !== -1) {
                    matched = keyword;
                    onKeyword(keyword, i, end);
                    i = end;
                    break;
                }
            }
            if (matched) {
                continue;
            }
        }
        i++;
    }
    return depth === 0;
}
function isWordBoundary(sql, index) {
    if (index === 0) {
        return true;
    }
    return !/[A-Za-z0-9_$]/.test(sql[index - 1]);
}
/**
 * Match `keyword` at `index` case-insensitively, allowing any run of
 * whitespace between the words of a multi-word keyword ("ORDER   BY").
 * Returns the index just past the keyword, or -1.
 */
function matchKeywordAt(sql, index, keyword) {
    let i = index;
    const words = keyword.split(' ');
    for (let w = 0; w < words.length; w++) {
        if (w > 0) {
            let sawSpace = false;
            while (i < sql.length && /\s/.test(sql[i])) {
                i++;
                sawSpace = true;
            }
            if (!sawSpace) {
                return -1;
            }
        }
        const word = words[w];
        if (sql.slice(i, i + word.length).toUpperCase() !== word) {
            return -1;
        }
        i += word.length;
    }
    // The keyword must be followed by a boundary, so "FROMAGE" is not "FROM".
    if (i < sql.length && /[A-Za-z0-9_$]/.test(sql[i])) {
        return -1;
    }
    return i;
}
function splitSelectStatement(sql) {
    const selectEnd = matchKeywordAt(sql, 0, 'SELECT');
    if (selectEnd === -1) {
        return undefined;
    }
    const marks = [];
    const balanced = scanTopLevel(sql, (keyword, start, end) => {
        marks.push({ keyword, start, end });
    });
    if (!balanced) {
        return undefined;
    }
    // Each clause keyword may appear at most once, and only in statement order.
    let previousRank = -1;
    for (const mark of marks) {
        const rank = SELECT_CLAUSE_KEYWORDS.indexOf(mark.keyword);
        if (rank <= previousRank) {
            return undefined;
        }
        previousRank = rank;
    }
    if (marks.length === 0 || marks[0].keyword !== 'FROM') {
        return undefined;
    }
    const sliceUntilNext = (markIndex) => {
        const start = marks[markIndex].end;
        const end = markIndex + 1 < marks.length ? marks[markIndex + 1].start : sql.length;
        return sql.slice(start, end).trim();
    };
    const clauses = {
        columns: sql.slice(selectEnd, marks[0].start).trim(),
        table: sliceUntilNext(0),
    };
    if (clauses.columns.length === 0) {
        return undefined;
    }
    for (let m = 1; m < marks.length; m++) {
        const text = sliceUntilNext(m);
        if (marks[m].keyword === 'WHERE') {
            clauses.where = text;
        }
        else if (marks[m].keyword === 'ORDER BY') {
            clauses.orderBy = text;
        }
        else {
            clauses.limit = text;
        }
    }
    return clauses;
}
/** Split on a delimiter that is outside quotes and at paren depth 0. */
function splitTopLevel(text, delimiter) {
    const parts = [];
    let last = 0;
    const balanced = scanTopLevel(text, undefined, (_delimiter, index) => {
        parts.push(text.slice(last, index));
        last = index + 1;
    }, [delimiter]);
    if (!balanced) {
        throw new Error('SQL parse error: unbalanced quotes or parentheses');
    }
    parts.push(text.slice(last));
    return parts;
}
function tokenizeWhere(input) {
    const tokens = [];
    let i = 0;
    while (i < input.length) {
        const ch = input[i];
        if (/\s/.test(ch)) {
            i++;
            continue;
        }
        if (ch === "'" || ch === '"') {
            const quote = ch;
            let value = '';
            let closed = false;
            i++;
            while (i < input.length) {
                if (input[i] === quote) {
                    if (input[i + 1] === quote) {
                        value += quote;
                        i += 2;
                        continue;
                    }
                    i++;
                    closed = true;
                    break;
                }
                value += input[i];
                i++;
            }
            if (!closed) {
                throw new Error('SQL parse error: unterminated string literal in WHERE');
            }
            tokens.push({ type: 'string', value });
            continue;
        }
        if (ch === '(' || ch === ')' || ch === ',') {
            tokens.push({ type: 'punc', value: ch });
            i++;
            continue;
        }
        const two = input.slice(i, i + 2);
        if (two === '<=' || two === '>=' || two === '<>' || two === '!=') {
            tokens.push({ type: 'op', value: two });
            i += 2;
            continue;
        }
        if (ch === '=' || ch === '<' || ch === '>') {
            tokens.push({ type: 'op', value: ch });
            i++;
            continue;
        }
        if (/[0-9]/.test(ch) || (ch === '-' && /[0-9]/.test(input[i + 1] ?? ''))) {
            let j = ch === '-' ? i + 1 : i;
            while (j < input.length && /[0-9]/.test(input[j])) {
                j++;
            }
            let isFloat = false;
            if (input[j] === '.' && /[0-9]/.test(input[j + 1] ?? '')) {
                isFloat = true;
                j++;
                while (j < input.length && /[0-9]/.test(input[j])) {
                    j++;
                }
            }
            if ((input[j] === 'e' || input[j] === 'E') && /[0-9+-]/.test(input[j + 1] ?? '')) {
                isFloat = true;
                j += 2;
                while (j < input.length && /[0-9]/.test(input[j])) {
                    j++;
                }
            }
            const raw = input.slice(i, j);
            if (isFloat) {
                tokens.push({ type: 'number', value: parseFloat(raw) });
            }
            else {
                // Security: preserve exactness beyond the safe integer range.
                const asBigInt = BigInt(raw);
                const exceedsSafe = asBigInt > BigInt(Number.MAX_SAFE_INTEGER) ||
                    asBigInt < BigInt(Number.MIN_SAFE_INTEGER);
                tokens.push({ type: 'number', value: exceedsSafe ? asBigInt : Number(raw) });
            }
            i = j;
            continue;
        }
        if (/[A-Za-z_]/.test(ch)) {
            let j = i;
            while (j < input.length && /[A-Za-z0-9_$.]/.test(input[j])) {
                j++;
            }
            tokens.push({ type: 'word', value: input.slice(i, j) });
            i = j;
            continue;
        }
        // Fail closed. An unrecognised character is never ignored.
        throw new Error('SQL parse error: unexpected character in WHERE clause');
    }
    return tokens;
}
// ---------------------------------------------------------------------------
// WHERE parser (recursive descent)
// ---------------------------------------------------------------------------
class WhereParser {
    tokens;
    pos = 0;
    constructor(tokens) {
        this.tokens = tokens;
    }
    parse() {
        const node = this.parseOr();
        if (this.pos < this.tokens.length) {
            throw new Error(`SQL parse error: unexpected token after WHERE expression: ${this.describe(this.peek())}`);
        }
        return node;
    }
    peek() {
        return this.tokens[this.pos];
    }
    describe(token) {
        if (!token) {
            return 'end of clause';
        }
        // Security: never echo literal values; identifiers and operators are
        // schema-level names, which the engine already surfaces in other errors.
        switch (token.type) {
            case 'word':
                return token.value;
            case 'op':
            case 'punc':
                return token.value;
            case 'string':
                return 'string literal';
            case 'number':
                return 'numeric literal';
        }
    }
    /** Consume the next token if it is the given keyword. */
    acceptKeyword(keyword) {
        const token = this.peek();
        if (token && token.type === 'word' && token.value.toUpperCase() === keyword) {
            this.pos++;
            return true;
        }
        return false;
    }
    expectKeyword(keyword) {
        if (!this.acceptKeyword(keyword)) {
            throw new Error(`SQL parse error: expected ${keyword}, found ${this.describe(this.peek())}`);
        }
    }
    acceptPunc(value) {
        const token = this.peek();
        if (token && token.type === 'punc' && token.value === value) {
            this.pos++;
            return true;
        }
        return false;
    }
    expectPunc(value) {
        if (!this.acceptPunc(value)) {
            throw new Error(`SQL parse error: expected '${value}', found ${this.describe(this.peek())}`);
        }
    }
    parseOr() {
        const children = [this.parseAnd()];
        while (this.acceptKeyword('OR')) {
            children.push(this.parseAnd());
        }
        return children.length === 1 ? children[0] : { kind: 'or', children };
    }
    parseAnd() {
        const children = [this.parseNot()];
        while (this.acceptKeyword('AND')) {
            children.push(this.parseNot());
        }
        return children.length === 1 ? children[0] : { kind: 'and', children };
    }
    parseNot() {
        if (this.acceptKeyword('NOT')) {
            return { kind: 'not', child: this.parseNot() };
        }
        return this.parsePrimary();
    }
    parsePrimary() {
        if (this.acceptPunc('(')) {
            const node = this.parseOr();
            this.expectPunc(')');
            return node;
        }
        return this.parsePredicate();
    }
    parsePredicate() {
        const token = this.peek();
        if (!token || token.type !== 'word') {
            throw new Error(`SQL parse error: expected a column name, found ${this.describe(token)}`);
        }
        const upper = token.value.toUpperCase();
        if (upper === 'AND' || upper === 'OR' || upper === 'NOT') {
            throw new Error(`SQL parse error: expected a column name, found ${upper}`);
        }
        const column = token.value;
        this.pos++;
        if (this.acceptKeyword('IS')) {
            const negated = this.acceptKeyword('NOT');
            this.expectKeyword('NULL');
            return { kind: 'isNull', column, negated };
        }
        const negated = this.acceptKeyword('NOT');
        if (this.acceptKeyword('BETWEEN')) {
            const minValue = this.parseLiteral();
            this.expectKeyword('AND');
            const maxValue = this.parseLiteral();
            return { kind: 'between', column, minValue, maxValue, negated };
        }
        if (this.acceptKeyword('IN')) {
            this.expectPunc('(');
            const values = [];
            if (!this.acceptPunc(')')) {
                values.push(this.parseLiteral());
                while (this.acceptPunc(',')) {
                    values.push(this.parseLiteral());
                }
                this.expectPunc(')');
            }
            return { kind: 'in', column, values, negated };
        }
        if (this.acceptKeyword('LIKE')) {
            const pattern = this.parseLiteral();
            if (typeof pattern !== 'string') {
                throw new Error('SQL parse error: LIKE requires a string pattern');
            }
            return { kind: 'like', column, matcher: likeToRegExp(pattern), negated };
        }
        if (negated) {
            throw new Error(`SQL parse error: expected BETWEEN, IN or LIKE after NOT, found ${this.describe(this.peek())}`);
        }
        const opToken = this.peek();
        if (!opToken || opToken.type !== 'op') {
            throw new Error(`SQL parse error: expected a comparison operator, found ${this.describe(opToken)}`);
        }
        this.pos++;
        return { kind: 'cmp', column, operator: opToken.value, value: this.parseLiteral() };
    }
    parseLiteral() {
        const token = this.peek();
        if (!token) {
            throw new Error('SQL parse error: expected a value, found end of clause');
        }
        if (token.type === 'string' || token.type === 'number') {
            this.pos++;
            return token.value;
        }
        if (token.type === 'word') {
            const upper = token.value.toUpperCase();
            if (upper === 'TRUE') {
                this.pos++;
                return true;
            }
            if (upper === 'FALSE') {
                this.pos++;
                return false;
            }
            if (upper === 'NULL') {
                this.pos++;
                return null;
            }
        }
        // Bare identifiers are not values: column-to-column comparison is not
        // supported, and silently treating one as a string would be a wrong answer.
        throw new Error(`SQL parse error: expected a literal value, found ${this.describe(token)}`);
    }
}
function parseWhereClause(clause) {
    const trimmed = clause.trim();
    if (trimmed.length === 0) {
        throw new Error('SQL parse error: empty WHERE clause');
    }
    return new WhereParser(tokenizeWhere(trimmed)).parse();
}
function parseOrderByClause(clause) {
    const terms = splitTopLevel(clause, ',')
        .map(part => part.trim())
        .filter(part => part.length > 0);
    if (terms.length === 0) {
        throw new Error('SQL parse error: empty ORDER BY clause');
    }
    return terms.map(term => {
        const match = term.match(/^([A-Za-z_][A-Za-z0-9_$.]*)(?:\s+(ASC|DESC))?$/i);
        if (!match) {
            // Fail closed: an ORDER BY the engine cannot honour is an error, not an
            // unordered result set.
            throw new Error('SQL parse error: unsupported ORDER BY term');
        }
        return {
            column: match[1],
            descending: (match[2] ?? '').toUpperCase() === 'DESC',
        };
    });
}
/**
 * Translate a SQL LIKE pattern into an anchored RegExp.
 *
 * `%` matches any run of characters, `_` matches exactly one. Matching is ASCII
 * case-insensitive, the SQLite default. Every other character is escaped, so a
 * pattern can never inject regex syntax, and runs of `%` are collapsed so the
 * compiled expression cannot backtrack pathologically.
 */
function likeToRegExp(pattern) {
    let source = '^';
    let previousWasWildcard = false;
    for (const ch of pattern) {
        if (ch === '%') {
            if (!previousWasWildcard) {
                source += '[\\s\\S]*';
                previousWasWildcard = true;
            }
            continue;
        }
        previousWasWildcard = false;
        if (ch === '_') {
            source += '[\\s\\S]';
        }
        else {
            source += ch.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        }
    }
    return new RegExp(`${source}$`, 'i');
}
// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------
function getRecordValue(record, column) {
    if (column === '_rowid') {
        return record.rowid;
    }
    if (column === '_offset') {
        return record.offset;
    }
    return record.fields.get(column);
}
function isMissing(value) {
    return value === null || value === undefined;
}
/** Numeric-aware equality that bridges the BigInt/Number split in stored fields. */
function valuesEqual(a, b) {
    if (typeof a === 'bigint' || typeof b === 'bigint') {
        if (typeof a === 'number' && !Number.isInteger(a)) {
            return false;
        }
        if (typeof b === 'number' && !Number.isInteger(b)) {
            return false;
        }
        if ((typeof a === 'bigint' || typeof a === 'number') &&
            (typeof b === 'bigint' || typeof b === 'number')) {
            return BigInt(a) === BigInt(b);
        }
        return false;
    }
    return a === b;
}
/** Ordering comparison; returns undefined when the values are not comparable. */
function compareValues(a, b) {
    if (isMissing(a) || isMissing(b)) {
        return undefined;
    }
    if (typeof a === 'bigint' || typeof b === 'bigint') {
        if ((typeof a === 'bigint' || typeof a === 'number') &&
            (typeof b === 'bigint' || typeof b === 'number')) {
            const left = typeof a === 'bigint' ? a : BigInt(Math.trunc(a));
            const right = typeof b === 'bigint' ? b : BigInt(Math.trunc(b));
            return left < right ? -1 : left > right ? 1 : 0;
        }
        return undefined;
    }
    if (typeof a === 'number' && typeof b === 'number') {
        return a < b ? -1 : a > b ? 1 : 0;
    }
    if (typeof a === 'string' && typeof b === 'string') {
        return a < b ? -1 : a > b ? 1 : 0;
    }
    if (typeof a === 'boolean' && typeof b === 'boolean') {
        return a === b ? 0 : a ? 1 : -1;
    }
    return undefined;
}
function evaluateWhere(record, node) {
    switch (node.kind) {
        case 'and':
            return node.children.every(child => evaluateWhere(record, child));
        case 'or':
            return node.children.some(child => evaluateWhere(record, child));
        case 'not':
            return !evaluateWhere(record, node.child);
        case 'isNull': {
            const missing = isMissing(getRecordValue(record, node.column));
            return node.negated ? !missing : missing;
        }
        case 'cmp': {
            const value = getRecordValue(record, node.column);
            if (node.operator === '=') {
                return valuesEqual(value, node.value);
            }
            if (node.operator === '!=' || node.operator === '<>') {
                // NULL is not "not equal" to anything, matching SQL's NULL semantics
                // closely enough that a filtered result never includes unknowns.
                if (isMissing(value)) {
                    return false;
                }
                return !valuesEqual(value, node.value);
            }
            const ordering = compareValues(value, node.value);
            if (ordering === undefined) {
                return false;
            }
            switch (node.operator) {
                case '<':
                    return ordering < 0;
                case '>':
                    return ordering > 0;
                case '<=':
                    return ordering <= 0;
                case '>=':
                    return ordering >= 0;
            }
            return false;
        }
        case 'between': {
            const value = getRecordValue(record, node.column);
            const low = compareValues(value, node.minValue);
            const high = compareValues(value, node.maxValue);
            if (low === undefined || high === undefined) {
                return false;
            }
            const inRange = low >= 0 && high <= 0;
            return node.negated ? !inRange : inRange;
        }
        case 'in': {
            const value = getRecordValue(record, node.column);
            if (isMissing(value)) {
                return false;
            }
            const found = node.values.some(candidate => valuesEqual(value, candidate));
            return node.negated ? !found : found;
        }
        case 'like': {
            const value = getRecordValue(record, node.column);
            if (isMissing(value)) {
                return false;
            }
            const matched = node.matcher.test(String(value));
            return node.negated ? !matched : matched;
        }
    }
}
// ---------------------------------------------------------------------------
// Planning
// ---------------------------------------------------------------------------
/**
 * Pick an indexed seek that is guaranteed to be a superset of the rows the
 * predicate can match. Only conjunctive (AND) context is safe: under OR, a row
 * may match through the other branch, so the seek would lose rows.
 */
function planIndexSeek(tableStore, node) {
    if (node.kind === 'cmp' && node.operator === '=' && tableStore.hasIndex(node.column)) {
        return { records: tableStore.findByIndex(node.column, node.value), exact: true };
    }
    if (node.kind === 'between' && !node.negated && tableStore.hasIndex(node.column)) {
        return {
            records: tableStore.findByRange(node.column, node.minValue, node.maxValue),
            exact: true,
        };
    }
    if (node.kind === 'and') {
        for (const child of node.children) {
            const seek = planIndexSeek(tableStore, child);
            if (seek) {
                // The remaining conjuncts still have to be checked.
                return { records: seek.records, exact: false };
            }
        }
    }
    return undefined;
}
function selectRecords(tableStore, where) {
    if (!where) {
        return tableStore.scanAll();
    }
    const seek = planIndexSeek(tableStore, where);
    if (seek?.exact) {
        return seek.records;
    }
    const candidates = seek ? seek.records : tableStore.scanAll();
    return candidates.filter(record => evaluateWhere(record, where));
}
function sortRecords(records, terms) {
    // Decorate with the original position so the sort is stable across engines.
    return records
        .map((record, index) => ({ record, index }))
        .sort((left, right) => {
        for (const term of terms) {
            const a = getRecordValue(left.record, term.column);
            const b = getRecordValue(right.record, term.column);
            // SQLite orders NULL first ascending; mirror that.
            const aMissing = isMissing(a);
            const bMissing = isMissing(b);
            if (aMissing || bMissing) {
                if (aMissing && bMissing) {
                    continue;
                }
                const nullsFirst = aMissing ? -1 : 1;
                return term.descending ? -nullsFirst : nullsFirst;
            }
            const ordering = compareValues(a, b);
            if (ordering === undefined || ordering === 0) {
                continue;
            }
            return term.descending ? -ordering : ordering;
        }
        return left.index - right.index;
    })
        .map(entry => entry.record);
}
//# sourceMappingURL=database.js.map