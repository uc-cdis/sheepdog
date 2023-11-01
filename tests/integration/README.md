TODO: Why are we integration testing alongside unit tests like this? We need to migrate
      these tests elsewhere b/c it's not really integration testing if we're
      configuring postgres exactly the way it expects in the CI running this. 
      Since this CI setup is completely separate from how we run postgres in 
      production, most of these tests are pointless.

      As of 13 SEP 23 I'm ripping out some of this to enable the migration 
      to Github Actions from Travis CI.

      - Alex