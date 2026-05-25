 # Companies Catalogue SFTP mirror
 
Cataloging companies house SFTP server bulk data products.

## Stages

- Crawl SFTP server (`crawler`)
  - saves a catalogue of all files on the server.
- Summarise catalogue 
- Save latest files of each data product.
  - get file in original format
  - upload to storage bucket compressed
