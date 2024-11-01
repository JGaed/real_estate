#!/bin/bash

kill $(pgrep -f snap/chromium/)
kill $(pgrep -f chromedriver)
kill $(pgrep -f chromium*)
