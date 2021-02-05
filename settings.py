from flask import Flask

# Flask app setup
app = Flask(__name__)

# DB configuration
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///./database.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
