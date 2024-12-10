You can create you own private key and self-signed certificate using OpenSSL to use in you own emulator. Here's how you can do it:

1. **Generate a Private Key:**
   Use OpenSSL to generate a new private key file. Here’s how you can do it:

   ```bash
   openssl genpkey -algorithm RSA -out oidc.key
   ```

   This command generates an RSA private key and saves it to `oidc.key`.

2. **Generate a Self-Signed Certificate:**
   Once you have the private key, you can generate a self-signed certificate using the `req` command, as you've shown:

   ```bash
   openssl req -new -x509 -key oidc.key -out oidc.cert -days 10000 -subj "/CN=oidc.federated-signon.cloud-tasks-emulator" -config "path/to/openssl.cnf"
   ```

   - `-new`: Generate a new certificate request.
   - `-x509`: Create a self-signed certificate.
   - `-key oidc.key`: Use the private key file `oidc.key`.
   - `-out oidc.cert`: Output the certificate to `oidc.cert`.
   - `-days 10000`: Validity of the certificate in days.
   - `-subj "/CN=oidc.federated-signon.cloud-tasks-emulator"`: Subject of the certificate. Adjust the Common Name (CN) as needed.
   - `-config "path/to/openssl.cnf"`: Path to your OpenSSL configuration file. Adjust this path according to your setup.

Make sure to replace `"path/to/openssl.cnf"` with the actual path to your OpenSSL configuration file on your system. This configuration file typically contains settings like default certificate extensions and other parameters relevant to certificate generation. Adjust the CN (Common Name) parameter (`/CN=`) to match your specific domain or server name.