
plugins {
    id("ktorm.base")
    id("ktorm.publish")
    id("ktorm.source-header-check")
}

dependencies {
    api(project(":ktorm-ksp-spi"))
    testImplementation(project(":ktorm-core"))
    api(project(":ktorm-ksp-annotations"))
}
