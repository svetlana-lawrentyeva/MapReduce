<?xml version="1.0" encoding="ISO-8859-1"?>
<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:ex="http://exslt.org/dates-and-times" extension-element-prefixes="ex"
        >

    <xsl:param name="ename">information</xsl:param>
    <xsl:param name="evalue">It's become dangerous!</xsl:param>
    <xsl:param name="attrname">date</xsl:param>

    <!--<xsl:output method="xml" indent="yes" />-->

    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
            <xsl:if test="((sell_rate > 25) and (money_type = 'dollar'))
                       or ((sell_rate > 30) and (money_type = 'euro'))">
                <xsl:element name="{$ename}">
                    <xsl:attribute name="{$attrname}">
                        <xsl:value-of select="ex:date()"/>
                    </xsl:attribute>
                    <xsl:value-of select="$evalue"/>
                </xsl:element>
            </xsl:if>
        </xsl:copy>
    </xsl:template>

    <xsl:template match="bank_code" />

    <!--<xsl:template match="text()">-->
        <!--<xsl:value-of select="normalize-space()" />-->
    <!--</xsl:template>-->

</xsl:stylesheet>