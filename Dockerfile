FROM ghcr.io/adomi-io/odoo:19.0

# Set user to root so we can install dependencies
USER root

RUN pip install "openai>=1.0.0" "pika>=1.3.0"

# Copy your custom addons into the container
COPY addons /volumes/addons
COPY config/odoo.conf /volumes/config/odoo.conf
COPY hooks/hook_setup.sh /hook_setup

# Provide Unovis UMD locally for the portal by copying from the Node build stage
RUN npm install @unovis/ts@^1.5.4

# Switch back to the non-root user
USER 1000