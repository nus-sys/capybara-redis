if [ "$1" = "demi" ]
then
	cd ../capybara && LIBOS=catnap make all-libs && cd ../redis && make distclean
fi

DEMIKERNEL_REPO_DIR=~/projects/Capybara/capybara make && LIBOS=catnap CONFIG_PATH=../capybara/config.yaml ./src/redis-server ./redis.conf
