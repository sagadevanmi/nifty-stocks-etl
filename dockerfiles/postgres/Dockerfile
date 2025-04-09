FROM postgres:14

# Set environment variables
ENV POSTGRES_PASSWORD q2wer56tgu8u!
ENV POSTGRES_HOST_AUTH_METHOD md5
ENV ACCEPT_EULA 1

# Copy custom pg_hba.conf
COPY pg_hba.conf postgres:/var/lib/postgresql/data/pg_hba.conf

# Update postgresql.conf to use our custom pg_hba.conf
# RUN echo "hba_file = '/etc/postgresql/pg_hba.conf'" >> /usr/share/postgresql/postgresql.conf.sample

# Set proper permissions
# RUN chmod 600 /etc/postgresql/pg_hba.conf 