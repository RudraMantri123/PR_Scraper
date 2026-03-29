#!/usr/bin/env python3
"""
Simply run: python database.py

This script will:
1. Check if verilog_repos.ndjson exists
2. Import it into SQLite database (if not already done)
3. Start a web dashboard for querying
4. Open your browser automatically
"""

import sqlite3
import json
import os
import sys
import webbrowser
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse


def check_and_import_data():
    """Check for data file and import if needed."""
    ndjson_file = "verilog_repos.ndjson"
    db_file = "github_repos.db"
    
    # Check if NDJSON file exists
    if not os.path.exists(ndjson_file):
        print(f"❌ Error: {ndjson_file} not found in current directory")
        print("Please make sure the NDJSON file is in the same folder as this script")
        return False
    
    # Check if database already exists and has data
    if os.path.exists(db_file):
        try:
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM repositories")
            count = cursor.fetchone()[0]
            conn.close()
            
            if count > 0:
                print(f"✅ Database already exists with {count:,} repositories")
                return True
        except:
            # Database exists but is corrupted or empty, reimport
            pass
    
    # Import data
    print(f"📥 Importing data from {ndjson_file}...")
    return import_ndjson_data(ndjson_file, db_file)


def import_ndjson_data(ndjson_file, db_file):
    """Import NDJSON data into SQLite database."""
    try:
        # Create database and tables
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS repositories (
                id INTEGER PRIMARY KEY,
                name TEXT,
                full_name TEXT,
                owner_login TEXT,
                owner_type TEXT,
                description TEXT,
                language TEXT,
                stargazers_count INTEGER,
                forks_count INTEGER,
                open_issues_count INTEGER,
                watchers_count INTEGER,
                size INTEGER,
                created_at TEXT,
                updated_at TEXT,
                pushed_at TEXT,
                homepage TEXT,
                html_url TEXT,
                clone_url TEXT,
                license_name TEXT,
                license_key TEXT,
                has_issues BOOLEAN,
                has_wiki BOOLEAN,
                has_pages BOOLEAN,
                has_downloads BOOLEAN,
                fork BOOLEAN,
                archived BOOLEAN,
                disabled BOOLEAN,
                private BOOLEAN,
                default_branch TEXT,
                topics TEXT,
                visibility TEXT
            )
        """)
        
        # Create indexes
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_language ON repositories (language)",
            "CREATE INDEX IF NOT EXISTS idx_stars ON repositories (stargazers_count)",
            "CREATE INDEX IF NOT EXISTS idx_owner ON repositories (owner_login)",
            "CREATE INDEX IF NOT EXISTS idx_license ON repositories (license_key)",
            "CREATE INDEX IF NOT EXISTS idx_issues ON repositories (has_issues)",
            "CREATE INDEX IF NOT EXISTS idx_fork ON repositories (fork)"
        ]
        
        for index in indexes:
            cursor.execute(index)
        
        conn.commit()
        
        # Import data
        with open(ndjson_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if line.strip():
                    try:
                        repo = json.loads(line)
                        
                        # Extract data
                        owner = repo.get('owner', {})
                        license_info = repo.get('license')
                        
                        cursor.execute("""
                            INSERT OR REPLACE INTO repositories 
                            (id, name, full_name, owner_login, owner_type, description, language,
                             stargazers_count, forks_count, open_issues_count, watchers_count, size,
                             created_at, updated_at, pushed_at, homepage, html_url, clone_url,
                             license_name, license_key, has_issues, has_wiki, has_pages, has_downloads,
                             fork, archived, disabled, private, default_branch, topics, visibility)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            repo.get('id'),
                            repo.get('name'),
                            repo.get('full_name'),
                            owner.get('login'),
                            owner.get('type'),
                            repo.get('description'),
                            repo.get('language'),
                            repo.get('stargazers_count', 0),
                            repo.get('forks_count', 0),
                            repo.get('open_issues_count', 0),
                            repo.get('watchers_count', 0),
                            repo.get('size', 0),
                            repo.get('created_at'),
                            repo.get('updated_at'),
                            repo.get('pushed_at'),
                            repo.get('homepage'),
                            repo.get('html_url'),
                            repo.get('clone_url'),
                            license_info.get('name') if license_info else None,
                            license_info.get('key') if license_info else None,
                            repo.get('has_issues', False),
                            repo.get('has_wiki', False),
                            repo.get('has_pages', False),
                            repo.get('has_downloads', False),
                            repo.get('fork', False),
                            repo.get('archived', False),
                            repo.get('disabled', False),
                            repo.get('private', False),
                            repo.get('default_branch'),
                            json.dumps(repo.get('topics', [])),
                            repo.get('visibility')
                        ))
                        
                        if line_num % 5000 == 0:
                            print(f"   Processed {line_num:,} repositories...")
                            conn.commit()
                            
                    except (json.JSONDecodeError, Exception):
                        continue
        
        conn.commit()
        conn.close()
        print(f"✅ Successfully imported {line_num:,} repositories!")
        return True
        
    except Exception as e:
        print(f"❌ Error importing data: {e}")
        return False


class DashboardHandler(BaseHTTPRequestHandler):
    """Web dashboard request handler."""
    
    def do_GET(self):
        """Handle GET requests."""
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)
        
        if parsed_url.path == '/':
            self._serve_dashboard(query_params)
        elif parsed_url.path == '/favicon.ico':
            self._serve_empty()
        else:
            self._serve_empty()
    
    def _serve_dashboard(self, query_params):
        """Serve main dashboard."""
        html = self._generate_html(query_params)
        
        self.send_response(200)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(html.encode('utf-8'))
    
    def _serve_empty(self):
        """Serve empty response."""
        self.send_response(204)
        self.end_headers()
    
    def _generate_html(self, query_params):
        """Generate dashboard HTML."""
        # Execute query if provided
        query_result = ""
        query_text = ""
        error_msg = ""
        
        if 'sql' in query_params and query_params['sql'][0].strip():
            query_text = query_params['sql'][0]
            try:
                results = self._execute_query(query_text)
                query_result = self._format_results(results)
            except Exception as e:
                error_msg = f"❌ Query Error: {str(e)}"
        
        # Get database stats
        stats_html = self._get_stats_html()
        
        return f"""
<!DOCTYPE html>
<html>
<head>
    <title>🔍 Verilog Repository Database</title>
    <meta charset="utf-8">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 20px 40px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        .header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
        }}
        .content {{
            padding: 30px;
            overflow-x: auto;
        }}
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: #f8f9fa;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
            border-left: 4px solid #667eea;
        }}
        .stat-number {{
            font-size: 2em;
            font-weight: bold;
            color: #667eea;
            margin-bottom: 5px;
        }}
        .stat-label {{
            color: #6c757d;
            font-size: 0.9em;
        }}
        .query-section {{
            background: #f8f9fa;
            padding: 25px;
            border-radius: 10px;
            margin-bottom: 20px;
        }}
        .query-section h2 {{
            margin-bottom: 15px;
            color: #495057;
        }}
        textarea {{
            width: 100%;
            height: 120px;
            padding: 15px;
            border: 2px solid #dee2e6;
            border-radius: 8px;
            font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
            font-size: 14px;
            resize: vertical;
            background: white;
        }}
        textarea:focus {{
            outline: none;
            border-color: #667eea;
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
        }}
        .btn {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 12px 25px;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            font-size: 16px;
            margin-top: 10px;
            transition: transform 0.2s;
        }}
        .btn:hover {{
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(102, 126, 234, 0.3);
        }}
        .examples {{
            margin-top: 15px;
        }}
        .example {{
            background: white;
            border: 1px solid #dee2e6;
            padding: 8px 12px;
            margin: 5px 5px 5px 0;
            border-radius: 5px;
            font-family: monospace;
            font-size: 12px;
            cursor: pointer;
            display: inline-block;
            transition: all 0.2s;
        }}
        .example:hover {{
            background: #e9ecef;
            border-color: #667eea;
        }}
        .error {{
            background: #f8d7da;
            color: #721c24;
            padding: 15px;
            border-radius: 8px;
            margin: 15px 0;
            border-left: 4px solid #dc3545;
        }}
        .results {{
            margin-top: 20px;
            width: 100%;
            overflow-x: auto;
        }}
        .table-container {{
            width: 100%;
            overflow-x: auto;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        table {{
            width: 100%;
            min-width: 800px;
            border-collapse: collapse;
        }}
        th {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 15px 10px;
            text-align: left;
            font-weight: 600;
            position: sticky;
            top: 0;
            z-index: 10;
        }}
        td {{
            padding: 12px 10px;
            border-bottom: 1px solid #dee2e6;
            white-space: nowrap;
            max-width: 300px;
            overflow: hidden;
            text-overflow: ellipsis;
        }}
        tr:hover {{
            background: #f8f9fa;
        }}
        .repo-link {{
            color: #667eea;
            text-decoration: none;
            font-weight: 500;
        }}
        .repo-link:hover {{
            text-decoration: underline;
        }}
        .result-count {{
            margin: 15px 0;
            padding: 10px;
            background: #d1ecf1;
            border-radius: 5px;
            color: #0c5460;
        }}
        .pagination {{
            margin: 20px 0;
            text-align: center;
        }}
        .pagination button {{
            background: #667eea;
            color: white;
            border: none;
            padding: 8px 16px;
            margin: 0 5px;
            border-radius: 5px;
            cursor: pointer;
        }}
        .pagination button:hover {{
            background: #5a6fd8;
        }}
        .pagination button:disabled {{
            background: #ccc;
            cursor: not-allowed;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔍 Verilog Repository Database</h1>
        </div>
        
        <div class="content">
            {stats_html}
            
            <div class="query-section">
                <h2>💻 SQL Query Interface</h2>
                <form method="GET">
                    <textarea name="sql" placeholder="Enter your SQL query here...">{query_text}</textarea>
                    <br>
                    <button type="submit" class="btn">Execute Query</button>
                </form>
                
                <div class="examples">
                    <strong>📝 Quick Examples (click to use):</strong><br>
                    <div class="example" onclick="setQuery(this.textContent)">SELECT full_name, open_issues_count, stargazers_count, forks_count, html_url FROM repositories WHERE has_issues = 1 ORDER BY open_issues_count DESC LIMIT 20</div>
                    <div class="example" onclick="setQuery(this.textContent)">SELECT full_name, stargazers_count, open_issues_count, forks_count, pushed_at, html_url FROM repositories WHERE stargazers_count > 100 AND open_issues_count > 10 ORDER BY stargazers_count DESC LIMIT 15</div>
                    <div class="example" onclick="setQuery(this.textContent)">SELECT full_name, forks_count, stargazers_count, open_issues_count, html_url FROM repositories WHERE forks_count > 20 ORDER BY forks_count DESC LIMIT 20</div>
                    <div class="example" onclick="setQuery(this.textContent)">SELECT full_name, pushed_at, open_issues_count, stargazers_count, html_url FROM repositories WHERE pushed_at > '2023-01-01' AND open_issues_count > 5 ORDER BY pushed_at DESC LIMIT 25</div>
                    <div class="example" onclick="setQuery(this.textContent)">SELECT full_name, open_issues_count, stargazers_count, forks_count, has_issues, html_url FROM repositories WHERE open_issues_count > 50 ORDER BY open_issues_count DESC LIMIT 15</div>
                    <div class="example" onclick="setQuery(this.textContent)">SELECT owner_type, COUNT(*) as repo_count, AVG(stargazers_count) as avg_stars, AVG(open_issues_count) as avg_issues FROM repositories GROUP BY owner_type ORDER BY repo_count DESC</div>
                    <div class="example" onclick="setQuery(this.textContent)">SELECT owner_login, owner_type, COUNT(*) as repo_count, SUM(stargazers_count) as total_stars, AVG(open_issues_count) as avg_issues FROM repositories GROUP BY owner_login ORDER BY repo_count DESC LIMIT 20</div>
                    <div class="example" onclick="setQuery(this.textContent)">SELECT full_name, has_wiki, has_pages, stargazers_count, open_issues_count, html_url FROM repositories WHERE has_wiki = 1 AND has_pages = 1 ORDER BY stargazers_count DESC LIMIT 15</div>
                    <div class="example" onclick="setQuery(this.textContent)">SELECT license_name, COUNT(*) as project_count, AVG(stargazers_count) as avg_stars, AVG(open_issues_count) as avg_issues FROM repositories WHERE license_name IS NOT NULL GROUP BY license_name ORDER BY project_count DESC LIMIT 10</div>
                    <div class="example" onclick="setQuery(this.textContent)">SELECT full_name, pushed_at, stargazers_count, open_issues_count, html_url FROM repositories WHERE pushed_at < '2022-01-01' AND stargazers_count > 10 ORDER BY stargazers_count DESC LIMIT 20</div>
                </div>
            </div>
            
            {error_msg and f'<div class="error">{error_msg}</div>' or ''}
            {query_result}
        </div>
    </div>
    
    <script>
        function setQuery(query) {{
            document.querySelector('textarea[name="sql"]').value = query;
        }}
    </script>
</body>
</html>
        """
    
    def _execute_query(self, query):
        """Execute SQL query."""
        conn = sqlite3.connect("github_repos.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        conn.close()
        return [dict(row) for row in results]
    
    def _format_results(self, results):
        """Format query results as HTML."""
        if not results:
            return '<div class="results"><div class="result-count">No results found.</div></div>'
        
        html = f'<div class="results"><div class="result-count">📊 Found {len(results):,} results</div>'
        html += '<div class="table-container"><table><tr>'
        
        # Headers
        headers = list(results[0].keys())
        for header in headers:
            html += f'<th>{header}</th>'
        html += '</tr>'
        
        # Rows (show all, up to 50 per page)
        for row in results[:50]:
            html += '<tr>'
            for header in headers:
                value = row[header]
                
                if header == 'html_url' and value:
                    repo_name = row.get('full_name', 'Repository')
                    html += f'<td><a href="{value}" target="_blank" class="repo-link" title="View Repository">{repo_name}</a></td>'
                elif header == 'full_name' and value and 'html_url' in row:
                    # Create contextual links based on the query
                    base_url = row['html_url']
                    repo_name = value
                    
                    # Check what type of query this might be based on other columns
                    links = [f'<a href="{base_url}" target="_blank" class="repo-link" title="Repository">{repo_name}</a>']
                    
                    if 'open_issues_count' in row or 'has_issues' in row:
                        links.append(f'<a href="{base_url}/issues" target="_blank" class="repo-link" title="Issues">📋</a>')
                    
                    if 'forks_count' in row:
                        links.append(f'<a href="{base_url}/network/members" target="_blank" class="repo-link" title="Forks">🍴</a>')
                    
                    if 'has_wiki' in row and row.get('has_wiki'):
                        links.append(f'<a href="{base_url}/wiki" target="_blank" class="repo-link" title="Wiki">📚</a>')
                    
                    if 'has_pages' in row and row.get('has_pages'):
                        links.append(f'<a href="https://{repo_name.split("/")[0]}.github.io/{repo_name.split("/")[1]}" target="_blank" class="repo-link" title="Pages">🌐</a>')
                    
                    # Add pulls link for PR-related queries
                    links.append(f'<a href="{base_url}/pulls" target="_blank" class="repo-link" title="Pull Requests">🔄</a>')
                    
                    html += f'<td>{" ".join(links)}</td>'
                elif header in ['stargazers_count', 'forks_count', 'open_issues_count', 'watchers_count'] and value is not None:
                    html += f'<td>{value:,}</td>'
                elif header == 'description' and value:
                    desc = str(value)[:100] + ('...' if len(str(value)) > 100 else '')
                    html += f'<td title="{str(value)}">{desc}</td>'
                elif header in ['created_at', 'updated_at', 'pushed_at'] and value:
                    # Format dates nicely
                    date_str = str(value)[:10] if len(str(value)) > 10 else str(value)
                    html += f'<td>{date_str}</td>'
                else:
                    display_value = str(value) if value is not None else ''
                    if len(display_value) > 50:
                        html += f'<td title="{display_value}">{display_value[:50]}...</td>'
                    else:
                        html += f'<td>{display_value}</td>'
            html += '</tr>'
        
        html += '</table></div>'
        
        if len(results) > 50:
            html += f'<div class="result-count">⚠️ Showing first 50 of {len(results):,} results. Consider adding LIMIT to your query for better performance.</div>'
        
        html += '</div>'
        return html
    
    def _get_stats_html(self):
        """Get database statistics HTML."""
        try:
            conn = sqlite3.connect("github_repos.db")
            cursor = conn.cursor()
            
            # Total repos
            cursor.execute("SELECT COUNT(*) FROM repositories")
            total_repos = cursor.fetchone()[0]
            
            # Repos with issues enabled
            cursor.execute("SELECT COUNT(*) FROM repositories WHERE has_issues = 1")
            repos_with_issues = cursor.fetchone()[0]
            
            # Repos with open issues
            cursor.execute("SELECT COUNT(*) FROM repositories WHERE open_issues_count > 0")
            repos_with_open_issues = cursor.fetchone()[0]
            
            # Licensed repos
            cursor.execute("SELECT COUNT(*) FROM repositories WHERE license_key IS NOT NULL")
            licensed_repos = cursor.fetchone()[0]
            
            conn.close()
            
            return f"""
            <div class="stats">
                <div class="stat-card">
                    <div class="stat-number">{total_repos:,}</div>
                    <div class="stat-label">Total Repositories</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">{repos_with_issues:,}</div>
                    <div class="stat-label">Issues Enabled</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">{repos_with_open_issues:,}</div>
                    <div class="stat-label">With Open Issues</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">{licensed_repos:,}</div>
                    <div class="stat-label">Licensed Repos</div>
                </div>
            </div>
            """
        except:
            return '<div class="stats"><div class="stat-card"><div class="stat-label">Stats loading...</div></div></div>'
    
    def log_message(self, format, *args):
        """Suppress server logs."""
        pass


def start_web_dashboard():
    """Start the web dashboard."""
    port = 8080
    
    # Find available port
    for p in range(8080, 8090):
        try:
            server = HTTPServer(('localhost', p), DashboardHandler)
            port = p
            break
        except OSError:
            continue
    else:
        print("❌ No available ports found")
        return
    
    print(f"🌐 Starting web dashboard at http://localhost:{port}")
    print("🚀 Opening browser...")
    print("📝 Press Ctrl+C to stop")
    
    # Open browser after short delay
    def open_browser():
        time.sleep(1.5)
        try:
            webbrowser.open(f"http://localhost:{port}")
        except:
            print("⚠️  Could not open browser automatically")
            print(f"   Please visit: http://localhost:{port}")
    
    threading.Thread(target=open_browser, daemon=True).start()
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n👋 Dashboard stopped")
        server.shutdown()


def main():
    """Main function - does everything automatically."""
    print("🔍 Verilog Repository Database")
    print("=" * 40)
    
    # Step 1: Check and import data
    if not check_and_import_data():
        print("❌ Failed to set up database")
        sys.exit(1)
    
    # Step 2: Start web dashboard
    print("\n🌐 Starting web dashboard...")
    start_web_dashboard()


if __name__ == "__main__":
    main()