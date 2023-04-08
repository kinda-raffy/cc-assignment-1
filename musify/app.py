import flask
import requests


app = flask.Flask(__name__)
app.secret_key = 's3897093@rmit'
gateway_url = "https://jtp42f3ocj.execute-api.us-east-1.amazonaws.com"
accounts_url = f"{gateway_url}/accounts"
content_url = f"{gateway_url}/content"


@app.route('/')
def landing_page():
    return flask.render_template('index.html')


@app.route('/login')
def login():
    """Renders the login page."""
    error = flask.session.pop('login_error', None)
    if error:
        return flask.render_template('login.html', error=error)
    return flask.render_template('login.html')


@app.route('/validate_login', methods=['POST'])
def validate_login():
    """Validates the user's login credentials."""
    email = flask.request.form['email']
    password = flask.request.form['password']
    # Validate input.
    if not all([email, password]):
        flask.session['login_error'] = "Email or password is missing"
        return flask.redirect('/login')
    # Send login request.
    response = requests.post(
        f"{accounts_url}/login",
        json={
            "email": email,
            "password": password
        }
    )
    # Handle errors.
    if response.status_code == 404:
        flask.session['login_error'] = "Email or password is invalid"
        return flask.redirect('/login')

    set_user_session_keys(response.json()['username'], email)
    return flask.redirect('/home')


@app.route('/logout_user')
def logout():
    """Logs the user out."""
    flask.session.pop('username', None)
    flask.session.pop('email', None)
    return flask.redirect('/login')


@app.route('/register')
def register():
    """Renders the register page."""
    error = flask.session.pop('register_error', None)
    if error:
        return flask.render_template('register.html', error=error)
    return flask.render_template('register.html')


@app.route('/validate_register', methods=['POST'])
def validate_register():
    """Validates the user's registration credentials."""
    email = flask.request.form['email']
    password = flask.request.form['password']
    username = flask.request.form['username']
    print("HELLOOOO", email, password, username)
    # Validate user input.
    if not all([email, password, username]):
        flask.session['register_error'] = "Email, password or username is missing"
        return flask.redirect('/register')
    # Send registration request.
    response = requests.post(
        f"{accounts_url}/register",
        json={
            "email": email,
            "password": password,
            "username": username
        }
    )
    # Handle errors.
    if response.status_code == 400:
        flask.session['register_error'] = "Email, password or username is missing"
        return flask.redirect('/register')
    elif response.status_code == 409:
        flask.session['register_error'] = "Email is already in use"
        return flask.redirect('/register')

    set_user_session_keys(username, email)
    return flask.redirect('/home')


def get_subscriptions():
    # Get the user's subscriptions.
    subscription_titles = requests.get(
        f"{accounts_url}/subscriptions",
        headers={
            "email": flask.session['email']
        }
    )
    # Get music information for each subscription.
    subscription_music = list()
    for title in subscription_titles.json():
        subscription_music.append(
            requests.get(f"{content_url}/?title={title}").json()
        )
    return flatten_list(subscription_music)


def flatten_list(list_of_lists):
    return [item for sublist in list_of_lists for item in sublist]


@app.route('/home')
def home():
    """Renders the main page."""
    # Validate user session.
    if 'username' not in flask.session:
        return flask.redirect('/login')
    # Get homepage resources.
    subscriptions = get_subscriptions()
    queried_music = flask.session.pop('queried_music', None)

    return flask.render_template(
        'home.html',
        user_name=flask.session['username'],
        subscribed_music=subscriptions,
        query_music=queried_music,
        removeMusic=remove_subscription,
    )


@app.route('/remove_subscription', methods=['POST'])
def remove_subscription():
    """Remove button handler."""
    title = flask.request.form['title']
    requests.delete(
        f"{accounts_url}/subscriptions",
        json={
            "email": flask.session['email'],
            "song_title": title
        }
    )
    return flask.redirect('/home')


@app.route('/add_subscription', methods=['POST'])
def add_subscription():
    """Subscribe button handler."""
    title = flask.request.form['title']
    requests.post(
        f"{accounts_url}/subscriptions",
        json={
            "email": flask.session['email'],
            "song_title": title
        }
    )
    return flask.redirect('/home')


@app.route('/query_music_form', methods=['POST'])
def query_music_form():
    """Query music form handler."""
    title = flask.request.form['title']
    artist = flask.request.form['artist']
    year = flask.request.form['year']

    # Validate user input.
    if not any([title, artist, year]):
        return flask.redirect('/home')

    # Build and send query.
    query = str()
    if title:
        query += f"title={title}"
    if artist:
        query += f"&artist={artist}"
    if year:
        query += f"&year={year}"

    queried_music = requests.get(
        f"{content_url}/?{query}"
    )

    flask.session['queried_music'] = queried_music.json()
    return flask.redirect('/home')


def set_user_session_keys(username, email):
    """Sets the user's session keys."""
    flask.session['username'] = username
    flask.session['email'] = email


if __name__ == '__main__':
    app.run()
