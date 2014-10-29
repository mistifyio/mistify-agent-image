mistify-agent-image
===================

Image service for mistify-agent


How to Test
-----------
The easiest way to test is to use the included vagrant file:

```
vagrant up
vagrant ssh
# then inside the vm
cd /home/vagrant/go/src/github.com/mistifyio/mistify-agent-image
sudo /usr/local/go/bin/go test
```
