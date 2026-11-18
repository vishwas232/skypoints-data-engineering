> Historical compatibility runbook. For the current native submission use [NATIVE_RUNBOOK.md](NATIVE_RUNBOOK.md) and [verification.md](verification.md).

# Publish this project using Windows Command Prompt

Create an **empty**, initially private repository named `skypoints-data-engineering` at https://github.com/new (do not initialize with a README).

Open CMD at the extracted `skypoints-data-engineering` folder and run:

```cmd
git --version
git init
git branch -M main
git status
```

Configure `git config --global user.name "Your Name"` and `git config --global user.email "you@example.com"` only if Git tells you to.

Commit phases separately:

```cmd
git add src/profile_inputs.py requirements.txt data/samples/ .gitignore
git commit -m "feat: add member source profiling"
git add sql/ README.md docs/ tests/
git commit -m "feat: define Snowflake staging and target schema"
```

Connect GitHub, replacing `YOUR_USERNAME`:

```cmd
git remote add origin https://github.com/YOUR_USERNAME/skypoints-data-engineering.git
git push -u origin main
git log --oneline
git status
```

These commits represent the contents of the assembled starter project; future commits should be made immediately after coding and testing each change. Never commit Snowflake passwords, access keys, tokens, or real production records.
