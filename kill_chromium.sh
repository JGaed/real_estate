#!/bin/bash

kill $(pgrep -f snap/chromium/)
kill $(pgrep -f chromedriver)