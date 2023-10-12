NLT tests are generating the following files:
- $DAOS\_SRC/nlt-errors.json: error in the stdouput and log files
- $DAOS\_SRC/nlt-server-leaks.json: server memory leaks detected
- /tmp/dnt\_*.log.bz2: output of the NLT commands.

The last log files can be used for discovering the NLT test in utils which is failing.
```bzgrep -e "ERROR MESSAGE" /tmp/dnt_*.log.bz2```
