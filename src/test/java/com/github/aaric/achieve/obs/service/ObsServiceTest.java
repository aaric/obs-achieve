package com.github.aaric.achieve.obs.service;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * ObsServiceTest
 *
 * @author Aaric, created on 2019-02-14T10:57.
 * @since 0.2.0-SNAPSHOT
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class ObsServiceTest {

    /**
     * 测试文件
     */
    private static final String testFileDirectory = FileUtils.getUserDirectoryPath() + "\\Desktop\\";
    private static final String testFileName = "404.jpg";

    @Autowired
    protected ObsService obsService;

    @Test
    public void testIsHasFile() {
        Assert.assertTrue(obsService.isHasFile("404.jpg"));
        Assert.assertFalse(obsService.isHasFile("404x.jpg"));
    }

    @Test
    public void testUploadFile() {

    }

    @Test
    public void testUploadFiles() {

    }

    @Test
    public void testDownloadFile() {

    }
}
