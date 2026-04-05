# Apify الافتراضي عند ربط GitHub (جذر الريبو): كاشط JSON-LD + Playwright
# Railway يستخدم Dockerfile.railway (انظر railway.json)
FROM apify/actor-python-playwright:3.13

USER myuser

COPY --chown=myuser:myuser apify_actor/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt \
    && python -m playwright install chromium

COPY --chown=myuser:myuser apify_actor/ ./
RUN python -m compileall -q my_actor/

CMD ["python", "-m", "my_actor"]
