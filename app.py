from flask import Flask, jsonify, request, send_from_directory, g, render_template
from flask_cors import CORS
import os
from datetime import datetime, timedelta
import secrets
import sqlite3
from contextlib import closing
import hashlib
import json

app = Flask(__name__)
CORS(app)
app.secret_key = os.environ.get('SECRET_KEY', secrets.token_hex(32))

# ========== DATABASE SETUP ==========
DATABASE = 'kaamkaro.db'

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def init_db():
    with app.app_context():
        db = get_db()
        
        # Users table
        db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                name TEXT,
                balance REAL DEFAULT 0.0,
                tasks_done INTEGER DEFAULT 0,
                total_earned REAL DEFAULT 0.0,
                joined DATE DEFAULT CURRENT_DATE,
                referral_code TEXT UNIQUE,
                referrals_count INTEGER DEFAULT 0,
                referral_earnings REAL DEFAULT 0.0,
                is_admin BOOLEAN DEFAULT 0,
                phone TEXT,
                status TEXT DEFAULT 'active',
                last_login TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Tasks table
        db.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT,
                reward REAL NOT NULL,
                type TEXT,
                duration INTEGER,
                status TEXT DEFAULT 'active',
                category TEXT,
                daily_limit INTEGER DEFAULT 1,
                total_completions INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Transactions table
        db.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                task_id INTEGER,
                task_title TEXT,
                amount REAL NOT NULL,
                type TEXT NOT NULL,
                description TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                balance_after REAL,
                withdrawal_id INTEGER,
                status TEXT DEFAULT 'completed',
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
        ''')
        
        # Withdrawals table
        db.execute('''
            CREATE TABLE IF NOT EXISTS withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                user_email TEXT,
                user_name TEXT,
                amount REAL NOT NULL,
                upi_id TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP,
                transaction_id TEXT UNIQUE,
                rejection_reason TEXT,
                method TEXT DEFAULT 'upi',
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
        ''')
        
        # Referrals table
        db.execute('''
            CREATE TABLE IF NOT EXISTS referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referred_id INTEGER,
                referral_code TEXT,
                earned_amount REAL DEFAULT 0.0,
                status TEXT DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (referrer_id) REFERENCES users (id),
                FOREIGN KEY (referred_id) REFERENCES users (id)
            )
        ''')
        
        # Daily login bonus
        db.execute('''
            CREATE TABLE IF NOT EXISTS daily_logins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                login_date DATE,
                streak_count INTEGER DEFAULT 1,
                bonus_amount REAL DEFAULT 0.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id),
                UNIQUE(user_id, login_date)
            )
        ''')
        
        db.commit()
        print("✅ Database tables created")
        
        # Insert demo data if empty
        cursor = db.execute('SELECT COUNT(*) FROM users')
        if cursor.fetchone()[0] == 0:
            insert_demo_data(db)
        
        cursor = db.execute('SELECT COUNT(*) FROM tasks')
        if cursor.fetchone()[0] == 0:
            insert_demo_tasks(db)

def insert_demo_data(db):
    # Hash passwords for security
    def hash_password(password):
        return hashlib.sha256(password.encode()).hexdigest()
    
    demo_users = [
        ('admin@kaamkaro.com', hash_password('admin123'), 'Admin User', 1000.0, 25, 1250.0, 
         '2024-01-01', 'ADMIN001', 10, 500.0, 1, '9876543210', 'active'),
        ('demo@kaamkaro.com', hash_password('demo123'), 'Demo User', 250.0, 15, 400.0, 
         '2024-01-15', 'DEMO001', 5, 250.0, 0, '9876543211', 'active'),
        ('user1@example.com', hash_password('user123'), 'Test User 1', 150.0, 8, 200.0,
         '2024-02-01', 'USER001', 2, 100.0, 0, '9876543212', 'active'),
        ('user2@example.com', hash_password('user123'), 'Test User 2', 75.0, 3, 100.0,
         '2024-02-10', 'USER002', 0, 0.0, 0, '9876543213', 'active')
    ]
    
    for user in demo_users:
        db.execute('''
            INSERT OR IGNORE INTO users 
            (email, password, name, balance, tasks_done, total_earned, 
             joined, referral_code, referrals_count, referral_earnings, is_admin, phone, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', user)
    
    db.commit()
    print("✅ Demo users inserted")

def insert_demo_tasks(db):
    demo_tasks = [
        ('📺 Watch YouTube Video', 'Watch a complete YouTube video (1-3 minutes)', 8.0, 
         'video', 180, 'active', 'entertainment', 5),
        ('📝 Complete Quick Survey', 'Answer 5 simple questions about products', 12.0, 
         'survey', 120, 'active', 'survey', 3),
        ('📱 Install & Open App', 'Install recommended app and use for 2 minutes', 25.0, 
         'install', 300, 'active', 'mobile', 2),
        ('📰 Read News Article', 'Read full article and answer question', 6.0, 
         'read', 90, 'active', 'education', 10),
        ('🎮 Play Mini Game', 'Play simple game and reach score 100', 15.0, 
         'game', 180, 'active', 'gaming', 4),
        ('🛒 Product Review', 'Review a product and give feedback', 20.0,
         'review', 240, 'active', 'shopping', 2),
        ('🎵 Listen to Music', 'Listen to complete song (3-5 minutes)', 5.0,
         'music', 300, 'active', 'entertainment', 8),
        ('📸 Upload Photo', 'Take and upload a photo with description', 10.0,
         'photo', 60, 'active', 'social', 5)
    ]
    
    for task in demo_tasks:
        db.execute('''
            INSERT INTO tasks 
            (title, description, reward, type, duration, status, category, daily_limit)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', task)
    
    db.commit()
    print("✅ Demo tasks inserted")

# Initialize database on startup
with app.app_context():
    init_db()

# ========== HELPER FUNCTIONS ==========
def row_to_dict(row):
    return dict(zip(row.keys(), row)) if row else None

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(stored_password, provided_password):
    return stored_password == hash_password(provided_password)

# ========== ROUTES ==========
@app.route('/')
def serve_home():
    return send_from_directory('.', 'index.html')

@app.route('/admin')
def serve_admin():
    return send_from_directory('.', 'admin.html')

@app.route('/<path:filename>')
def serve_static(filename):
    return send_from_directory('.', filename)

# ========== API ENDPOINTS ==========
@app.route('/api/health', methods=['GET'])
def health_check():
    db = get_db()
    
    users_count = db.execute('SELECT COUNT(*) FROM users').fetchone()[0]
    tasks_count = db.execute('SELECT COUNT(*) FROM tasks WHERE status="active"').fetchone()[0]
    total_balance = db.execute('SELECT SUM(balance) FROM users').fetchone()[0] or 0
    total_withdrawals = db.execute('SELECT SUM(amount) FROM withdrawals WHERE status="approved"').fetchone()[0] or 0
    pending_withdrawals = db.execute('SELECT SUM(amount) FROM withdrawals WHERE status="pending"').fetchone()[0] or 0
    
    return jsonify({
        "status": "healthy",
        "app": "Watch & Earn - KaamKaro Pro",
        "version": "4.0.0",
        "timestamp": datetime.now().isoformat(),
        "stats": {
            "total_users": users_count,
            "active_tasks": tasks_count,
            "total_balance": total_balance,
            "total_withdrawals": total_withdrawals,
            "pending_withdrawals": pending_withdrawals,
            "uptime": "100%"
        }
    })

@app.route('/api/dashboard/stats', methods=['GET'])
def dashboard_stats():
    db = get_db()
    
    # Total stats
    total_users = db.execute('SELECT COUNT(*) FROM users').fetchone()[0]
    active_users = db.execute('SELECT COUNT(*) FROM users WHERE status="active"').fetchone()[0]
    total_tasks = db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0]
    active_tasks = db.execute('SELECT COUNT(*) FROM tasks WHERE status="active"').fetchone()[0]
    
    # Earnings stats
    total_earned = db.execute('SELECT SUM(total_earned) FROM users').fetchone()[0] or 0
    total_withdrawn = db.execute('SELECT SUM(amount) FROM withdrawals WHERE status="approved"').fetchone()[0] or 0
    platform_balance = total_earned - total_withdrawn
    
    # Today's stats
    today = datetime.now().strftime('%Y-%m-%d')
    today_earnings = db.execute('SELECT SUM(amount) FROM transactions WHERE DATE(timestamp) = ? AND type="task_completion"', (today,)).fetchone()[0] or 0
    today_users = db.execute('SELECT COUNT(*) FROM users WHERE DATE(created_at) = ?', (today,)).fetchone()[0]
    today_withdrawals = db.execute('SELECT SUM(amount) FROM withdrawals WHERE DATE(requested_at) = ?', (today,)).fetchone()[0] or 0
    
    # Recent activities
    recent_transactions = db.execute('''
        SELECT t.*, u.name as user_name FROM transactions t
        JOIN users u ON t.user_id = u.id
        ORDER BY t.timestamp DESC LIMIT 5
    ''')
    recent_transactions = [row_to_dict(row) for row in recent_transactions.fetchall()]
    
    recent_withdrawals = db.execute('''
        SELECT * FROM withdrawals 
        ORDER BY requested_at DESC LIMIT 5
    ''')
    recent_withdrawals = [row_to_dict(row) for row in recent_withdrawals.fetchall()]
    
    return jsonify({
        "success": True,
        "stats": {
            "total_users": total_users,
            "active_users": active_users,
            "total_tasks": total_tasks,
            "active_tasks": active_tasks,
            "total_earned": total_earned,
            "total_withdrawn": total_withdrawn,
            "platform_balance": platform_balance,
            "today_earnings": today_earnings,
            "today_users": today_users,
            "today_withdrawals": today_withdrawals
        },
        "recent_activities": {
            "transactions": recent_transactions,
            "withdrawals": recent_withdrawals
        }
    })

@app.route('/api/tasks', methods=['GET'])
def get_all_tasks():
    db = get_db()
    cursor = db.execute('SELECT * FROM tasks WHERE status="active" ORDER BY reward DESC')
    tasks = [row_to_dict(row) for row in cursor.fetchall()]
    
    return jsonify({
        "success": True,
        "tasks": tasks,
        "count": len(tasks),
        "message": "Tasks loaded successfully"
    })

@app.route('/api/tasks/<int:task_id>', methods=['GET'])
def get_single_task(task_id):
    db = get_db()
    cursor = db.execute('SELECT * FROM tasks WHERE id = ?', (task_id,))
    task = row_to_dict(cursor.fetchone())
    
    if task:
        return jsonify({"success": True, "task": task})
    return jsonify({"success": False, "error": "Task not found"}), 404

@app.route('/api/register', methods=['POST'])
def register():
    try:
        data = request.get_json()
        email = data.get('email', '').strip().lower()
        password = data.get('password', '')
        name = data.get('name', email.split('@')[0].title())
        phone = data.get('phone', '')
        referral_code = data.get('referral_code', '').upper()
        
        if not email or not password:
            return jsonify({"success": False, "error": "Email and password required"}), 400
        
        if len(password) < 6:
            return jsonify({"success": False, "error": "Password must be at least 6 characters"}), 400
        
        db = get_db()
        
        # Check if email exists
        cursor = db.execute('SELECT id FROM users WHERE email = ?', (email,))
        if cursor.fetchone():
            return jsonify({"success": False, "error": "Email already registered"}), 400
        
        # Generate referral code
        user_referral_code = f"REF{secrets.token_hex(3).upper()}"
        
        # Hash password
        hashed_password = hash_password(password)
        
        # Insert user
        db.execute('''
            INSERT INTO users 
            (email, password, name, balance, referral_code, phone)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (email, hashed_password, name, 50.0, user_referral_code, phone))
        
        user_id = db.execute('SELECT last_insert_rowid()').fetchone()[0]
        
        # Process referral if provided
        if referral_code:
            cursor = db.execute('SELECT id FROM users WHERE referral_code = ?', (referral_code,))
            referrer = cursor.fetchone()
            if referrer:
                referrer_id = referrer[0]
                # Add referral record
                db.execute('''
                    INSERT INTO referrals (referrer_id, referred_id, referral_code)
                    VALUES (?, ?, ?)
                ''', (referrer_id, user_id, referral_code))
                
                # Update referrer's stats
                db.execute('''
                    UPDATE users SET 
                    referrals_count = referrals_count + 1,
                    referral_earnings = referral_earnings + 50.0,
                    balance = balance + 50.0
                    WHERE id = ?
                ''', (referrer_id,))
                
                # Add referral transaction
                db.execute('''
                    INSERT INTO transactions 
                    (user_id, amount, type, description, balance_after)
                    SELECT ?, 50.0, 'referral_bonus', 
                    'Referral bonus from ' || email, balance + 50.0
                    FROM users WHERE id = ?
                ''', (referrer_id, referrer_id))
        
        # Welcome bonus transaction
        db.execute('''
            INSERT INTO transactions 
            (user_id, amount, type, description, balance_after)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, 50.0, 'signup_bonus', 'Welcome bonus for new registration', 50.0))
        
        db.commit()
        
        # Get user data
        cursor = db.execute('SELECT * FROM users WHERE id = ?', (user_id,))
        user = row_to_dict(cursor.fetchone())
        
        # Remove password
        user_response = {k: v for k, v in user.items() if k != 'password'}
        
        return jsonify({
            "success": True,
            "message": "Account created successfully! ₹50 welcome bonus credited.",
            "user": user_response,
            "token": f"token-{user_id}-{secrets.token_hex(8)}",
            "bonus": 50.0
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json()
    email = data.get('email', '').strip().lower()
    password = data.get('password', '')
    
    if not email or not password:
        return jsonify({"success": False, "error": "Email and password required"}), 400
    
    db = get_db()
    
    # Check if user exists
    cursor = db.execute('SELECT * FROM users WHERE email = ?', (email,))
    user = cursor.fetchone()
    
    if user:
        user_dict = row_to_dict(user)
        
        # Verify password
        if not verify_password(user_dict.get('password'), password):
            return jsonify({"success": False, "error": "Invalid password"}), 401
        
        # Update last login
        db.execute('UPDATE users SET last_login = ? WHERE id = ?', 
                  (datetime.now().isoformat(), user_dict['id']))
        
        # Check daily login bonus
        today = datetime.now().strftime('%Y-%m-%d')
        cursor = db.execute('SELECT * FROM daily_logins WHERE user_id = ? AND login_date = ?', 
                           (user_dict['id'], today))
        daily_login = cursor.fetchone()
        
        if not daily_login:
            # Calculate streak
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            cursor = db.execute('SELECT streak_count FROM daily_logins WHERE user_id = ? AND login_date = ?',
                               (user_dict['id'], yesterday))
            yesterday_login = cursor.fetchone()
            
            streak_count = 1
            if yesterday_login:
                streak_count = yesterday_login[0] + 1
            
            # Calculate bonus (₹10 per day of streak, max ₹70)
            bonus_amount = min(streak_count * 10, 70)
            
            # Add daily bonus
            db.execute('''
                INSERT INTO daily_logins (user_id, login_date, streak_count, bonus_amount)
                VALUES (?, ?, ?, ?)
            ''', (user_dict['id'], today, streak_count, bonus_amount))
            
            # Update user balance
            db.execute('UPDATE users SET balance = balance + ? WHERE id = ?', 
                      (bonus_amount, user_dict['id']))
            
            # Add transaction
            db.execute('''
                INSERT INTO transactions 
                (user_id, amount, type, description, balance_after)
                SELECT ?, ?, ?, ?, balance + ?
                FROM users WHERE id = ?
            ''', (user_dict['id'], bonus_amount, 'daily_bonus', 
                  f'Daily login bonus (Day {streak_count})', bonus_amount, user_dict['id']))
        
        db.commit()
        
        # Remove password from response
        user_response = {k: v for k, v in user_dict.items() if k != 'password'}
        
        return jsonify({
            "success": True,
            "message": "Login successful",
            "user": user_response,
            "token": f"token-{user_dict['id']}-{secrets.token_hex(8)}",
            "daily_bonus": bonus_amount if 'bonus_amount' in locals() else 0
        })
    
    return jsonify({"success": False, "error": "User not found"}), 404

@app.route('/api/tasks/complete', methods=['POST'])
def complete_task():
    data = request.get_json()
    user_id = data.get('user_id')
    task_id = data.get('task_id')
    
    if not user_id or not task_id:
        return jsonify({"success": False, "error": "User ID and Task ID required"}), 400
    
    db = get_db()
    
    # Get user
    cursor = db.execute('SELECT * FROM users WHERE id = ?', (user_id,))
    user = row_to_dict(cursor.fetchone())
    
    if not user:
        return jsonify({"success": False, "error": "User not found"}), 404
    
    # Get task
    cursor = db.execute('SELECT * FROM tasks WHERE id = ?', (task_id,))
    task = row_to_dict(cursor.fetchone())
    
    if not task:
        return jsonify({"success": False, "error": "Task not found"}), 404
    
    if task.get('status') != 'active':
        return jsonify({"success": False, "error": "Task is not available"}), 400
    
    # Check daily limit
    today = datetime.now().strftime('%Y-%m-%d')
    cursor = db.execute('''
        SELECT COUNT(*) FROM transactions 
        WHERE user_id = ? AND task_id = ? AND DATE(timestamp) = ?
    ''', (user_id, task_id, today))
    
    daily_count = cursor.fetchone()[0]
    daily_limit = task.get('daily_limit', 1)
    
    if daily_count >= daily_limit:
        return jsonify({
            "success": False, 
            "error": f"Daily limit reached for this task (Max: {daily_limit})"
        }), 400
    
    # Process reward
    reward = task.get('reward', 0)
    new_balance = user.get('balance', 0) + reward
    
    # Update user
    db.execute('''
        UPDATE users SET 
        balance = ?, 
        tasks_done = tasks_done + 1, 
        total_earned = total_earned + ?
        WHERE id = ?
    ''', (new_balance, reward, user_id))
    
    # Update task completion count
    db.execute('UPDATE tasks SET total_completions = total_completions + 1 WHERE id = ?', (task_id,))
    
    # Create transaction
    db.execute('''
        INSERT INTO transactions 
        (user_id, task_id, task_title, amount, type, description, balance_after)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (user_id, task_id, task['title'], reward, 'task_completion',
          f"Completed: {task['title']}", new_balance))
    
    db.commit()
    
    # Get updated user
    cursor = db.execute('SELECT * FROM users WHERE id = ?', (user_id,))
    updated_user = row_to_dict(cursor.fetchone())
    
    # Remove password
    user_response = {k: v for k, v in updated_user.items() if k != 'password'}
    
    return jsonify({
        "success": True,
        "message": f"Task completed! ₹{reward} credited to your account.",
        "reward": reward,
        "new_balance": new_balance,
        "user": user_response
    })

@app.route('/api/user/<int:user_id>', methods=['GET'])
def get_user_profile(user_id):
    db = get_db()
    
    cursor = db.execute('SELECT * FROM users WHERE id = ?', (user_id,))
    user = row_to_dict(cursor.fetchone())
    
    if not user:
        return jsonify({"success": False, "error": "User not found"}), 404
    
    # Remove password
    user_response = {k: v for k, v in user.items() if k != 'password'}
    
    # Get user transactions (last 20)
    cursor = db.execute('''
        SELECT * FROM transactions 
        WHERE user_id = ? 
        ORDER BY timestamp DESC 
        LIMIT 20
    ''', (user_id,))
    transactions = [row_to_dict(row) for row in cursor.fetchall()]
    
    # Get user withdrawals
    cursor = db.execute('''
        SELECT * FROM withdrawals 
        WHERE user_id = ? 
        ORDER BY requested_at DESC 
        LIMIT 10
    ''', (user_id,))
    withdrawals = [row_to_dict(row) for row in cursor.fetchall()]
    
    # Get referral stats
    cursor = db.execute('''
        SELECT COUNT(*) as total_refs, SUM(earned_amount) as total_earned
        FROM referrals WHERE referrer_id = ?
    ''', (user_id,))
    ref_stats = row_to_dict(cursor.fetchone())
    
    # Get daily login streak
    cursor = db.execute('''
        SELECT streak_count FROM daily_logins 
        WHERE user_id = ? 
        ORDER BY login_date DESC 
        LIMIT 1
    ''', (user_id,))
    streak_row = cursor.fetchone()
    streak_count = streak_row[0] if streak_row else 0
    
    return jsonify({
        "success": True,
        "user": user_response,
        "transactions": transactions,
        "withdrawals": withdrawals,
        "stats": {
            "total_earned": user.get('total_earned', 0),
            "tasks_completed": user.get('tasks_done', 0),
            "referral_count": ref_stats.get('total_refs', 0) if ref_stats else 0,
            "referral_earnings": ref_stats.get('total_earned', 0) if ref_stats else 0,
            "daily_streak": streak_count
        }
    })

@app.route('/api/withdraw/request', methods=['POST'])
def withdraw_request():
    data = request.get_json()
    user_id = data.get('user_id')
    amount = float(data.get('amount', 0))
    upi_id = data.get('upi_id', '').strip()
    method = data.get('method', 'upi')
    
    if not user_id or not amount or not upi_id:
        return jsonify({"success": False, "error": "All fields are required"}), 400
    
    if amount < 100:
        return jsonify({"success": False, "error": "Minimum withdrawal amount is ₹100"}), 400
    
    if amount > 10000:
        return jsonify({"success": False, "error": "Maximum withdrawal amount is ₹10,000"}), 400
    
    if method == 'upi' and '@' not in upi_id:
        return jsonify({"success": False, "error": "Invalid UPI ID format"}), 400
    
    db = get_db()
    
    # Get user
    cursor = db.execute('SELECT * FROM users WHERE id = ?', (user_id,))
    user = row_to_dict(cursor.fetchone())
    
    if not user:
        return jsonify({"success": False, "error": "User not found"}), 404
    
    if user['balance'] < amount:
        return jsonify({"success": False, "error": "Insufficient balance"}), 400
    
    # Check daily withdrawal limit
    today = datetime.now().strftime('%Y-%m-%d')
    cursor = db.execute('''
        SELECT SUM(amount) FROM withdrawals 
        WHERE user_id = ? AND DATE(requested_at) = ? AND status IN ('pending', 'approved')
    ''', (user_id, today))
    today_withdrawals = cursor.fetchone()[0] or 0
    
    if today_withdrawals + amount > 5000:
        return jsonify({"success": False, "error": "Daily withdrawal limit exceeded (Max: ₹5000)"}), 400
    
    # Deduct amount
    new_balance = user['balance'] - amount
    db.execute('UPDATE users SET balance = ? WHERE id = ?', (new_balance, user_id))
    
    # Create withdrawal record
    transaction_id = f"WT{datetime.now().strftime('%Y%m%d')}{secrets.token_hex(4).upper()}"
    
    cursor = db.execute('''
        INSERT INTO withdrawals 
        (user_id, user_email, user_name, amount, upi_id, transaction_id, method)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (user_id, user['email'], user['name'], amount, upi_id, transaction_id, method))
    
    withdrawal_id = cursor.lastrowid
    
    # Create transaction record
    db.execute('''
        INSERT INTO transactions 
        (user_id, amount, type, description, balance_after, withdrawal_id)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (user_id, amount, 'withdrawal_request', 
          f"Withdrawal request to {upi_id} ({method.upper()})", new_balance, withdrawal_id))
    
    db.commit()
    
    # Get withdrawal record
    cursor = db.execute('SELECT * FROM withdrawals WHERE id = ?', (withdrawal_id,))
    withdrawal = row_to_dict(cursor.fetchone())
    
    return jsonify({
        "success": True,
        "message": f"Withdrawal request for ₹{amount} submitted successfully",
        "withdrawal": withdrawal,
        "new_balance": new_balance
    })

@app.route('/api/referral/stats/<int:user_id>', methods=['GET'])
def referral_stats(user_id):
    db = get_db()
    
    # Get referral list
    cursor = db.execute('''
        SELECT r.*, u.email as referred_email, u.name as referred_name, u.created_at as signup_date
        FROM referrals r
        JOIN users u ON r.referred_id = u.id
        WHERE r.referrer_id = ?
        ORDER BY r.created_at DESC
    ''', (user_id,))
    referrals = [row_to_dict(row) for row in cursor.fetchall()]
    
    # Get referral summary
    cursor = db.execute('''
        SELECT 
            COUNT(*) as total_referrals,
            SUM(earned_amount) as total_earnings,
            COUNT(CASE WHEN DATE(r.created_at) = DATE('now') THEN 1 END) as today_referrals
        FROM referrals r
        WHERE r.referrer_id = ?
    ''', (user_id,))
    summary = row_to_dict(cursor.fetchone())
    
    return jsonify({
        "success": True,
        "referrals": referrals,
        "summary": summary
    })

# ========== ADMIN ENDPOINTS ==========
@app.route('/api/admin/dashboard', methods=['GET'])
def admin_dashboard():
    db = get_db()
    
    # Overall stats
    total_users = db.execute('SELECT COUNT(*) FROM users').fetchone()[0]
    total_balance = db.execute('SELECT SUM(balance) FROM users').fetchone()[0] or 0
    total_withdrawn = db.execute('SELECT SUM(amount) FROM withdrawals WHERE status="approved"').fetchone()[0] or 0
    pending_withdrawals = db.execute('SELECT SUM(amount) FROM withdrawals WHERE status="pending"').fetchone()[0] or 0
    
    # Today's stats
    today = datetime.now().strftime('%Y-%m-%d')
    today_signups = db.execute('SELECT COUNT(*) FROM users WHERE DATE(created_at) = ?', (today,)).fetchone()[0]
    today_earnings = db.execute('SELECT SUM(amount) FROM transactions WHERE DATE(timestamp) = ? AND type="task_completion"', (today,)).fetchone()[0] or 0
    today_withdrawals = db.execute('SELECT SUM(amount) FROM withdrawals WHERE DATE(requested_at) = ? AND status="approved"', (today,)).fetchone()[0] or 0
    
    # Weekly stats
    week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    weekly_signups = db.execute('SELECT COUNT(*) FROM users WHERE DATE(created_at) >= ?', (week_ago,)).fetchone()[0]
    weekly_earnings = db.execute('SELECT SUM(amount) FROM transactions WHERE DATE(timestamp) >= ? AND type="task_completion"', (week_ago,)).fetchone()[0] or 0
    
    # Recent activities
    recent_users = db.execute('SELECT id, name, email, balance, created_at FROM users ORDER BY id DESC LIMIT 5')
    recent_users = [row_to_dict(row) for row in recent_users.fetchall()]
    
    recent_transactions = db.execute('''
        SELECT t.*, u.name as user_name FROM transactions t
        JOIN users u ON t.user_id = u.id
        ORDER BY t.timestamp DESC LIMIT 10
    ''')
    recent_transactions = [row_to_dict(row) for row in recent_transactions.fetchall()]
    
    recent_withdrawals = db.execute('''
        SELECT w.*, u.name as user_name FROM withdrawals w
        JOIN users u ON w.user_id = u.id
        ORDER BY w.requested_at DESC LIMIT 10
    ''')
    recent_withdrawals = [row_to_dict(row) for row in recent_withdrawals.fetchall()]
    
    # Top earners
    top_earners = db.execute('''
        SELECT id, name, email, total_earned, balance, tasks_done 
        FROM users 
        ORDER BY total_earned DESC 
        LIMIT 5
    ''')
    top_earners = [row_to_dict(row) for row in top_earners.fetchall()]
    
    # Popular tasks
    popular_tasks = db.execute('''
        SELECT id, title, reward, total_completions, daily_limit
        FROM tasks 
        ORDER BY total_completions DESC 
        LIMIT 5
    ''')
    popular_tasks = [row_to_dict(row) for row in popular_tasks.fetchall()]
    
    return jsonify({
        "success": True,
        "stats": {
            "total_users": total_users,
            "total_balance": total_balance,
            "total_withdrawn": total_withdrawn,
            "pending_withdrawals": pending_withdrawals,
            "today_signups": today_signups,
            "today_earnings": today_earnings,
            "today_withdrawals": today_withdrawals,
            "weekly_signups": weekly_signups,
            "weekly_earnings": weekly_earnings
        },
        "recent_activities": {
            "users": recent_users,
            "transactions": recent_transactions,
            "withdrawals": recent_withdrawals
        },
        "leaderboards": {
            "top_earners": top_earners,
            "popular_tasks": popular_tasks
        }
    })

@app.route('/api/admin/users', methods=['GET'])
def admin_get_users():
    db = get_db()
    cursor = db.execute('SELECT * FROM users ORDER BY id DESC')
    users = [row_to_dict(row) for row in cursor.fetchall()]
    
    # Remove passwords
    for user in users:
        user.pop('password', None)
    
    return jsonify({"success": True, "users": users, "count": len(users)})

@app.route('/api/admin/users/<int:user_id>', methods=['GET'])
def admin_get_user(user_id):
    db = get_db()
    cursor = db.execute('SELECT * FROM users WHERE id = ?', (user_id,))
    user = row_to_dict(cursor.fetchone())
    
    if not user:
        return jsonify({"success": False, "error": "User not found"}), 404
    
    user.pop('password', None)
    
    # Get user transactions
    cursor = db.execute('''
        SELECT * FROM transactions 
        WHERE user_id = ? 
        ORDER BY timestamp DESC
    ''', (user_id,))
    transactions = [row_to_dict(row) for row in cursor.fetchall()]
    
    # Get user withdrawals
    cursor = db.execute('''
        SELECT * FROM withdrawals 
        WHERE user_id = ? 
        ORDER BY requested_at DESC
    ''', (user_id,))
    withdrawals = [row_to_dict(row) for row in cursor.fetchall()]
    
    return jsonify({
        "success": True,
        "user": user,
        "transactions": transactions,
        "withdrawals": withdrawals
    })

@app.route('/api/admin/users/<int:user_id>/update', methods=['POST'])
def admin_update_user(user_id):
    data = request.get_json()
    
    db = get_db()
    
    # Check if user exists
    cursor = db.execute('SELECT id FROM users WHERE id = ?', (user_id,))
    if not cursor.fetchone():
        return jsonify({"success": False, "error": "User not found"}), 404
    
    # Update fields
    update_fields = []
    update_values = []
    
    if 'balance' in data:
        update_fields.append('balance = ?')
        update_values.append(float(data['balance']))
    
    if 'status' in data:
        update_fields.append('status = ?')
        update_values.append(data['status'])
    
    if 'is_admin' in data:
        update_fields.append('is_admin = ?')
        update_values.append(1 if data['is_admin'] else 0)
    
    if update_fields:
        update_values.append(user_id)
        query = f'UPDATE users SET {", ".join(update_fields)} WHERE id = ?'
        db.execute(query, update_values)
        db.commit()
    
    return jsonify({"success": True, "message": "User updated successfully"})

@app.route('/api/admin/tasks', methods=['GET'])
def admin_get_tasks():
    db = get_db()
    cursor = db.execute('SELECT * FROM tasks ORDER BY id DESC')
    tasks = [row_to_dict(row) for row in cursor.fetchall()]
    return jsonify({"success": True, "tasks": tasks, "count": len(tasks)})

@app.route('/api/admin/tasks/create', methods=['POST'])
def admin_create_task():
    data = request.get_json()
    
    required_fields = ['title', 'description', 'reward']
    for field in required_fields:
        if field not in data or not data[field]:
            return jsonify({"success": False, "error": f"{field} is required"}), 400
    
    db = get_db()
    
    db.execute('''
        INSERT INTO tasks 
        (title, description, reward, type, duration, status, category, daily_limit)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        data['title'],
        data['description'],
        float(data['reward']),
        data.get('type', 'general'),
        int(data.get('duration', 60)),
        data.get('status', 'active'),
        data.get('category', 'general'),
        int(data.get('daily_limit', 1))
    ))
    
    db.commit()
    return jsonify({"success": True, "message": "Task created successfully"})

@app.route('/api/admin/tasks/<int:task_id>/update', methods=['POST'])
def admin_update_task(task_id):
    data = request.get_json()
    
    db = get_db()
    
    # Check if task exists
    cursor = db.execute('SELECT id FROM tasks WHERE id = ?', (task_id,))
    if not cursor.fetchone():
        return jsonify({"success": False, "error": "Task not found"}), 404
    
    # Update fields
    update_fields = []
    update_values = []
    
    fields = ['title', 'description', 'reward', 'type', 'duration', 'status', 'category', 'daily_limit']
    for field in fields:
        if field in data:
            update_fields.append(f'{field} = ?')
            if field in ['reward', 'duration', 'daily_limit']:
                update_values.append(float(data[field]) if field == 'reward' else int(data[field]))
            else:
                update_values.append(data[field])
    
    if update_fields:
        update_values.append(task_id)
        query = f'UPDATE tasks SET {", ".join(update_fields)} WHERE id = ?'
        db.execute(query, update_values)
        db.commit()
    
    return jsonify({"success": True, "message": "Task updated successfully"})

@app.route('/api/admin/withdrawals', methods=['GET'])
def admin_get_withdrawals():
    db = get_db()
    cursor = db.execute('''
        SELECT w.*, u.name as user_name, u.email as user_email 
        FROM withdrawals w
        JOIN users u ON w.user_id = u.id
        ORDER BY w.requested_at DESC
    ''')
    withdrawals = [row_to_dict(row) for row in cursor.fetchall()]
    return jsonify({"success": True, "withdrawals": withdrawals, "count": len(withdrawals)})

@app.route('/api/admin/withdrawals/stats', methods=['GET'])
def admin_withdrawal_stats():
    db = get_db()
    
    # Status counts
    cursor = db.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
            SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) as approved,
            SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) as rejected,
            SUM(amount) as total_amount,
            SUM(CASE WHEN status = 'approved' THEN amount ELSE 0 END) as approved_amount,
            SUM(CASE WHEN status = 'pending' THEN amount ELSE 0 END) as pending_amount
        FROM withdrawals
    ''')
    stats = row_to_dict(cursor.fetchone())
    
    # Daily stats (last 7 days)
    daily_stats = []
    for i in range(7):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        cursor = db.execute('''
            SELECT 
                COUNT(*) as count,
                SUM(amount) as amount,
                SUM(CASE WHEN status = 'approved' THEN amount ELSE 0 END) as approved_amount
            FROM withdrawals 
            WHERE DATE(requested_at) = ?
        ''', (date,))
        day_stats = row_to_dict(cursor.fetchone())
        day_stats['date'] = date
        daily_stats.append(day_stats)
    
    return jsonify({
        "success": True,
        "stats": stats,
        "daily_stats": daily_stats
    })

@app.route('/api/admin/withdrawals/<int:withdrawal_id>/approve', methods=['POST'])
def approve_withdrawal(withdrawal_id):
    data = request.get_json()
    admin_notes = data.get('notes', 'Approved by admin')
    
    db = get_db()
    
    cursor = db.execute('SELECT * FROM withdrawals WHERE id = ?', (withdrawal_id,))
    withdrawal = row_to_dict(cursor.fetchone())
    
    if not withdrawal:
        return jsonify({"success": False, "error": "Withdrawal not found"}), 404
    
    if withdrawal['status'] != 'pending':
        return jsonify({"success": False, "error": "Withdrawal already processed"}), 400
    
    # Update status
    db.execute('''
        UPDATE withdrawals SET 
        status = 'approved', 
        processed_at = ?,
        rejection_reason = ?
        WHERE id = ?
    ''', (datetime.now().isoformat(), admin_notes, withdrawal_id))
    
    # Update transaction description
    db.execute('''
        UPDATE transactions SET 
        description = ?
        WHERE withdrawal_id = ?
    ''', (f"Withdrawal approved - {admin_notes}", withdrawal_id))
    
    db.commit()
    
    cursor = db.execute('SELECT * FROM withdrawals WHERE id = ?', (withdrawal_id,))
    updated_withdrawal = row_to_dict(cursor.fetchone())
    
    return jsonify({
        "success": True,
        "message": f"Withdrawal #{withdrawal_id} approved successfully",
        "withdrawal": updated_withdrawal
    })

@app.route('/api/admin/withdrawals/<int:withdrawal_id>/reject', methods=['POST'])
def reject_withdrawal(withdrawal_id):
    data = request.get_json()
    reason = data.get('reason', 'No reason provided')
    
    db = get_db()
    
    cursor = db.execute('SELECT * FROM withdrawals WHERE id = ?', (withdrawal_id,))
    withdrawal = row_to_dict(cursor.fetchone())
    
    if not withdrawal:
        return jsonify({"success": False, "error": "Withdrawal not found"}), 404
    
    if withdrawal['status'] != 'pending':
        return jsonify({"success": False, "error": "Withdrawal already processed"}), 400
    
    # Return money to user
    cursor = db.execute('SELECT balance FROM users WHERE id = ?', (withdrawal['user_id'],))
    user_balance = cursor.fetchone()[0]
    new_balance = user_balance + withdrawal['amount']
    
    db.execute('UPDATE users SET balance = ? WHERE id = ?', 
              (new_balance, withdrawal['user_id']))
    
    # Update withdrawal status
    db.execute('''
        UPDATE withdrawals SET 
        status = 'rejected', 
        rejection_reason = ?,
        processed_at = ?
        WHERE id = ?
    ''', (reason, datetime.now().isoformat(), withdrawal_id))
    
    # Update transaction
    db.execute('''
        UPDATE transactions SET 
        description = ?,
        amount = -amount
        WHERE withdrawal_id = ?
    ''', (f"Withdrawal rejected - {reason}", withdrawal_id))
    
    # Add refund transaction
    db.execute('''
        INSERT INTO transactions 
        (user_id, amount, type, description, balance_after, withdrawal_id)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (withdrawal['user_id'], withdrawal['amount'], 'withdrawal_refund',
          f"Withdrawal #{withdrawal_id} refunded: {reason}", new_balance, withdrawal_id))
    
    db.commit()
    
    cursor = db.execute('SELECT * FROM withdrawals WHERE id = ?', (withdrawal_id,))
    updated_withdrawal = row_to_dict(cursor.fetchone())
    
    return jsonify({
        "success": True,
        "message": f"Withdrawal #{withdrawal_id} rejected",
        "withdrawal": updated_withdrawal
    })

@app.route('/api/admin/transactions', methods=['GET'])
def admin_get_transactions():
    db = get_db()
    
    # Get query parameters
    limit = request.args.get('limit', 50, type=int)
    offset = request.args.get('offset', 0, type=int)
    user_id = request.args.get('user_id', type=int)
    
    query = '''
        SELECT t.*, u.name as user_name, u.email as user_email 
        FROM transactions t
        JOIN users u ON t.user_id = u.id
    '''
    params = []
    
    if user_id:
        query += ' WHERE t.user_id = ?'
        params.append(user_id)
    
    query += ' ORDER BY t.timestamp DESC LIMIT ? OFFSET ?'
    params.extend([limit, offset])
    
    cursor = db.execute(query, params)
    transactions = [row_to_dict(row) for row in cursor.fetchall()]
    
    # Total count
    count_query = 'SELECT COUNT(*) FROM transactions'
    if user_id:
        count_query += ' WHERE user_id = ?'
        cursor = db.execute(count_query, [user_id] if user_id else [])
    else:
        cursor = db.execute(count_query)
    
    total = cursor.fetchone()[0]
    
    return jsonify({
        "success": True,
        "transactions": transactions,
        "total": total,
        "limit": limit,
        "offset": offset
    })

@app.route('/api/admin/analytics', methods=['GET'])
def admin_analytics():
    db = get_db()
    
    # User growth (last 30 days)
    user_growth = []
    for i in range(30):
        date = (datetime.now() - timedelta(days=29-i)).strftime('%Y-%m-%d')
        cursor = db.execute('SELECT COUNT(*) FROM users WHERE DATE(created_at) <= ?', (date,))
        total_users = cursor.fetchone()[0]
        user_growth.append({"date": date, "users": total_users})
    
    # Earnings by day (last 7 days)
    daily_earnings = []
    for i in range(7):
        date = (datetime.now() - timedelta(days=6-i)).strftime('%Y-%m-%d')
        cursor = db.execute('''
            SELECT 
                SUM(CASE WHEN type = 'task_completion' THEN amount ELSE 0 END) as task_earnings,
                SUM(CASE WHEN type = 'signup_bonus' THEN amount ELSE 0 END) as signup_bonus,
                SUM(CASE WHEN type = 'referral_bonus' THEN amount ELSE 0 END) as referral_bonus,
                SUM(CASE WHEN type = 'daily_bonus' THEN amount ELSE 0 END) as daily_bonus
            FROM transactions 
            WHERE DATE(timestamp) = ?
        ''', (date,))
        earnings = row_to_dict(cursor.fetchone())
        earnings['date'] = date
        daily_earnings.append(earnings)
    
    # Task completion stats
    task_stats = db.execute('''
        SELECT 
            COUNT(*) as total_completions,
            SUM(amount) as total_earnings,
            AVG(amount) as avg_earning
        FROM transactions 
        WHERE type = 'task_completion'
    ''')
    task_stats = row_to_dict(task_stats.fetchone())
    
    # Withdrawal stats
    withdrawal_stats = db.execute('''
        SELECT 
            COUNT(*) as total_withdrawals,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount,
            SUM(CASE WHEN status = 'pending' THEN amount ELSE 0 END) as pending_amount,
            SUM(CASE WHEN status = 'approved' THEN amount ELSE 0 END) as approved_amount
        FROM withdrawals
    ''')
    withdrawal_stats = row_to_dict(withdrawal_stats.fetchone())
    
    return jsonify({
        "success": True,
        "analytics": {
            "user_growth": user_growth,
            "daily_earnings": daily_earnings,
            "task_stats": task_stats,
            "withdrawal_stats": withdrawal_stats
        }
    })

# ========== ERROR HANDLERS ==========
@app.errorhandler(404)
def not_found(error):
    return jsonify({"success": False, "error": "Route not found"}), 404

@app.errorhandler(500)
def server_error(error):
    return jsonify({"success": False, "error": "Internal server error"}), 500

# ========== APPLICATION START ==========
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print("\n" + "="*70)
    print("🚀 KAM KARO PRO - PRODUCTION SERVER")
    print("="*70)
    print(f"✅ Database: {DATABASE}")
    print(f"👤 Demo Admin: admin@kaamkaro.com / admin123")
    print(f"👤 Demo User: demo@kaamkaro.com / demo123")
    print(f"💰 Welcome Bonus: ₹50 for new users")
    print(f"📱 Frontend: http://localhost:{port}")
    print(f"⚙️ Admin Panel: http://localhost:{port}/admin")
    print(f"🔧 API Health: http://localhost:{port}/api/health")
    print("="*70)
    print("📊 Features:")
    print("  • User Registration & Login")
    print("  • Task Completion System")
    print("  • Referral Program (₹50 per referral)")
    print("  • Daily Login Bonus")
    print("  • UPI Withdrawals")
    print("  • Admin Dashboard")
    print("  • Real-time Analytics")
    print("="*70 + "\n")
    
    app.run(host='0.0.0.0', port=port, debug=False)