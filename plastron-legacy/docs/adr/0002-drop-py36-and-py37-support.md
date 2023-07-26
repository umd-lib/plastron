# 0002 - Drop Python 3.6 and 3.7 Support

Date: November 7, 2022

## Context

The official end-of-support date for Python 3.6 was December 23, 2021. See
[PEP 494 § Lifespan](https://peps.python.org/pep-0494/#lifespan)
for the official statement.

The projected end-of-support date for Python 3.7 is June 27, 2023. See
[PEP 537 § Lifespan](https://peps.python.org/pep-0537/#lifespan)
for the official timeline.

## Decision

**Change the minimum supported Python version to 3.8,** which is still
supported. Its projected end-of-support date is October 2024. See
[PEP 569 § Lifespan](https://peps.python.org/pep-0569/#lifespan).

While Python 3.7 has not yet reached its official end-of-life date, that date
only 8 months away at the time of this writing. Additionally, there are some
packages used by Plastron that do not have good support for Python 3.7 on the
Macbook arm64 platform now in use by UMD Libraries' developers.

**Update the version of Python used in the Docker image to 3.11,** which is
the most recent stable version of Python. It is projected to receive bugfixes 
until October 2023, and it will remain supported by security fixes until
October 2027. See [PEP 664 § Lifespan](https://peps.python.org/pep-0664/#lifespan).

[Features implemented in Python 3.8](https://peps.python.org/pep-0569/#features-for-3-8)

**Use of Python 3.8+ features MUST only occur in Plastron 4.x.** Any Plastron 3.x
release MUST be runnable on Python 3.6 on the amd64 platform.

**Plastron 3.x releases after 3.7.0 SHOULD provide a warning to the user if the 
detected Python version is less than 3.8.**

## Consequences

Non-Docker-based systems that use Plastron 3.x with Python 3.6 SHOULD update to 
Python 3.8 or later.

Non-Docker-based systems that use Plastron 4.x MUST have Python 3.8 or later.
